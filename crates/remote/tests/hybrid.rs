use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256};
use alloy_provider::{ProviderBuilder, mock::Asserter, network::AnyNetwork};
use alloy_rpc_types_eth::{Block, Filter, Header, Log};
use evm_fork_cache::reactive::{
    BlockRef, ChainControl, ChainStatus, DeliveryAudience, DeliveryScope, EventSubscriber,
    HandlerError, HandlerId, HandlerOutcome, InputSource, InterestOwnerSubscriber, LogInterest,
    ReactiveConfig, ReactiveContext, ReactiveEffect, ReactiveEngine, ReactiveHandler,
    ReactiveInput, ReactiveInputBatch, ReactiveInputDelivery, ReactiveInputRecord,
    ReactiveInterest, StateEffectQuality, SubscriberBackfill, SubscriberCapabilities,
    SubscriberCapability, SubscriberCheckpoint, SubscriberDeliveryToken, SubscriberError,
    SubscriberNextBatch, SubscriberOperation, SubscriberPayloadCommitment,
    SubscriberResumePosition,
};
use evm_fork_cache::{
    DurableCheckpointBlock, DurableCheckpointIdentity, DurableCheckpointMetadata, EvmCache,
    ReactiveRuntime, SlotDelta, StateUpdate, StateView,
};
use evm_fork_cache_remote::{
    HYBRID_MAX_CANONICAL_HISTORY, HYBRID_MAX_HANDLER_ID_BYTES, HYBRID_MAX_RECENT_INPUTS,
    HYBRID_MAX_RECENT_OWNER_ENTRIES, HYBRID_MAX_SOURCE_CHECKPOINT_BYTES,
    HYBRID_MAX_SOURCE_DELIVERY_TOKEN_BYTES, HybridConfig, HybridPhase, HybridSubscriber,
};

#[allow(clippy::large_enum_variant)] // Readable, short-lived integration-test scripts.
enum Step {
    Batch(Duration, ReactiveInputBatch<Ethereum>),
    Error(Duration, &'static str),
    End(Duration),
}

struct ScriptedSubscriber {
    name: &'static str,
    steps: VecDeque<Step>,
    acknowledgements: Arc<Mutex<Vec<Vec<u8>>>>,
    registrations: Arc<Mutex<Vec<&'static str>>>,
    polls: Option<Arc<AtomicUsize>>,
}

/// Live source whose post-bootstrap deliveries become visible only after a
/// dynamic owner registration commits. This models the provider race where the
/// chain advances while the historical service is activating a new revision.
struct RegistrationGatedLiveSubscriber {
    initial: Option<ReactiveInputBatch<Ethereum>>,
    active_steps: VecDeque<Step>,
    activated: bool,
    acknowledgements: Arc<Mutex<Vec<Vec<u8>>>>,
    registrations: Arc<Mutex<Vec<&'static str>>>,
}

struct ExactTopologySubscriber {
    historical: bool,
    steps: VecDeque<Step>,
    owners: Arc<Mutex<Vec<HandlerId>>>,
    replace_results: VecDeque<Result<(), &'static str>>,
}

struct LifecycleMethodProbeSubscriber {
    historical: bool,
    calls: Arc<Mutex<Vec<&'static str>>>,
    steps: VecDeque<Step>,
}

struct DestructiveTopologySubscriber {
    historical: bool,
    steps: VecDeque<Step>,
    topology: Arc<Mutex<Vec<HandlerId>>>,
    lifecycle_calls: Arc<Mutex<Vec<(bool, u64)>>>,
    global_calls: usize,
    exact_calls: usize,
}

impl DestructiveTopologySubscriber {
    fn install(
        topology: &Arc<Mutex<Vec<HandlerId>>>,
        owners: &[(HandlerId, Vec<ReactiveInterest<Ethereum>>)],
    ) {
        let mut ids = owners
            .iter()
            .map(|(owner, _)| owner.clone())
            .collect::<Vec<_>>();
        ids.sort();
        *topology.lock().unwrap() = ids;
    }
}

impl EventSubscriber<Ethereum> for DestructiveTopologySubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let mut capabilities = vec![
            SubscriberCapability::Logs,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
            SubscriberCapability::Barriers,
        ];
        if self.historical {
            capabilities.extend([
                SubscriberCapability::HistoricalBackfill,
                SubscriberCapability::DurableReplay,
            ]);
        } else {
            capabilities.push(SubscriberCapability::Live);
        }
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            let Some(step) = self.steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self.steps.pop_front().expect("peeked destructive step") {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }
}

impl InterestOwnerSubscriber<Ethereum> for DestructiveTopologySubscriber {
    fn upsert_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            Self::install(&self.topology, &owners);
            Ok(())
        })
    }

    fn replace_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            Self::install(&self.topology, &owners);
            self.exact_calls += 1;
            if !self.historical {
                // Exact replacement models Alloy's destructive reset: queued
                // events/cursor state disappear. The compensation replacement
                // then starts a new live epoch whose first observable item is
                // block 103.
                self.steps.clear();
                if self.exact_calls >= 2 {
                    self.steps
                        .push_back(Step::Batch(Duration::ZERO, tokenless_batch(&[103])));
                }
            }
            Ok(())
        })
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            Self::install(&self.topology, &owners);
            self.global_calls += 1;
            self.lifecycle_calls
                .lock()
                .unwrap()
                .push((self.historical, backfill.start_block()));
            if self.historical && self.global_calls == 1 {
                // The service committed the new desired state but the caller
                // observed an uncertain transport failure.
                return Err(SubscriberError::Provider(
                    "uncertain historical commit".into(),
                ));
            }
            if self.historical {
                self.steps.push_back(Step::Batch(
                    Duration::from_millis(10),
                    // Destructive replacement starts a new child token
                    // namespace, so reusing the pre-reset raw token is valid.
                    batch(&[102], b"history-100"),
                ));
            }
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        self.upsert_interest_owners(vec![(owner, interests.to_vec())])
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        _owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        Box::pin(async { Ok(None) })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

impl LifecycleMethodProbeSubscriber {
    fn label(&self, historical: &'static str, live: &'static str) -> &'static str {
        if self.historical { historical } else { live }
    }
}

impl EventSubscriber<Ethereum> for LifecycleMethodProbeSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let mut capabilities = vec![
            SubscriberCapability::Logs,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
            SubscriberCapability::Barriers,
        ];
        if self.historical {
            capabilities.extend([
                SubscriberCapability::HistoricalBackfill,
                SubscriberCapability::DurableReplay,
            ]);
        } else {
            capabilities.push(SubscriberCapability::Live);
        }
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            let Some(step) = self.steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self.steps.pop_front().expect("peeked probe step") {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }
}

impl InterestOwnerSubscriber<Ethereum> for LifecycleMethodProbeSubscriber {
    fn upsert_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push(self.label("history:upsert", "live:upsert"));
            Ok(())
        })
    }

    fn replace_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push(self.label("history:replace", "live:replace"));
            Ok(())
        })
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(self.label(
                "history:replace-global-backfill",
                "live:replace-global-backfill",
            ));
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push(self.label("history:add", "live:add"));
            Ok(())
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push(self.label("history:add-with-backfill", "live:add-with-backfill"));
            Ok(())
        })
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
        _retained: BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(self.label(
                "history:add-with-canonical-catchup",
                "live:add-with-canonical-catchup",
            ));
            Ok(())
        })
    }

    fn remove_interest_owner(
        &mut self,
        _owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        Box::pin(async { Ok(None) })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

impl ExactTopologySubscriber {
    fn store_owners(
        owners: &Arc<Mutex<Vec<HandlerId>>>,
        replacement: &[(HandlerId, Vec<ReactiveInterest<Ethereum>>)],
    ) {
        let mut ids = replacement
            .iter()
            .map(|(owner, _)| owner.clone())
            .collect::<Vec<_>>();
        ids.sort();
        *owners.lock().unwrap() = ids;
    }
}

impl EventSubscriber<Ethereum> for ExactTopologySubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let mut capabilities = vec![
            SubscriberCapability::Logs,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
            SubscriberCapability::Barriers,
        ];
        if self.historical {
            capabilities.extend([
                SubscriberCapability::HistoricalBackfill,
                SubscriberCapability::DurableReplay,
            ]);
        } else {
            capabilities.push(SubscriberCapability::Live);
        }
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            let Some(step) = self.steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self.steps.pop_front().expect("peeked topology step") {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }
}

impl InterestOwnerSubscriber<Ethereum> for ExactTopologySubscriber {
    fn upsert_interest_owners(
        &mut self,
        owner_updates: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            let mut owners = self.owners.lock().unwrap();
            for (owner, _) in owner_updates {
                if !owners.contains(&owner) {
                    owners.push(owner);
                }
            }
            owners.sort();
            Ok(())
        })
    }

    fn replace_interest_owners(
        &mut self,
        replacement: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            Self::store_owners(&self.owners, &replacement);
            match self.replace_results.pop_front().unwrap_or(Ok(())) {
                Ok(()) => Ok(()),
                Err(message) => Err(SubscriberError::Provider(message.into())),
            }
        })
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        replacement: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.replace_interest_owners(replacement)
    }

    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        self.upsert_interest_owners(vec![(owner, Vec::new())])
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _retained: BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        let owner = owner.clone();
        Box::pin(async move {
            self.owners
                .lock()
                .unwrap()
                .retain(|candidate| candidate != &owner);
            Ok(None)
        })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

impl RegistrationGatedLiveSubscriber {
    fn new(
        initial: ReactiveInputBatch<Ethereum>,
        active_steps: impl IntoIterator<Item = Step>,
        acknowledgements: Arc<Mutex<Vec<Vec<u8>>>>,
        registrations: Arc<Mutex<Vec<&'static str>>>,
    ) -> Self {
        Self {
            initial: Some(initial),
            active_steps: active_steps.into_iter().collect(),
            activated: false,
            acknowledgements,
            registrations,
        }
    }
}

impl EventSubscriber<Ethereum> for RegistrationGatedLiveSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        SubscriberCapabilities::new([
            SubscriberCapability::Live,
            SubscriberCapability::Logs,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
            SubscriberCapability::ExplicitReorgs,
        ])
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.registrations.lock().unwrap().push("live");
            Ok(())
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            if let Some(initial) = self.initial.take() {
                return Ok(Some(initial));
            }
            if !self.activated {
                std::future::pending::<()>().await;
                unreachable!("a cancelled pre-activation poll is retried after registration");
            }
            let Some(step) = self.active_steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self.active_steps.pop_front().expect("peeked live step") {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.acknowledgements
                .lock()
                .unwrap()
                .push(token.into_bytes());
            Ok(())
        })
    }
}

impl InterestOwnerSubscriber<Ethereum> for RegistrationGatedLiveSubscriber {
    fn upsert_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.activated = true;
            self.registrations.lock().unwrap().push("live");
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        self.upsert_interest_owners(Vec::new())
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        _owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        Box::pin(async { Ok(None) })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

impl ScriptedSubscriber {
    fn new(
        name: &'static str,
        steps: impl IntoIterator<Item = Step>,
        acknowledgements: Arc<Mutex<Vec<Vec<u8>>>>,
        registrations: Arc<Mutex<Vec<&'static str>>>,
    ) -> Self {
        Self {
            name,
            steps: steps.into_iter().collect(),
            acknowledgements,
            registrations,
            polls: None,
        }
    }

    fn with_poll_counter(mut self, polls: Arc<AtomicUsize>) -> Self {
        self.polls = Some(polls);
        self
    }
}

impl EventSubscriber<Ethereum> for ScriptedSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let mut capabilities = vec![
            SubscriberCapability::Logs,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
            SubscriberCapability::Barriers,
            SubscriberCapability::ExplicitReorgs,
        ];
        if self.name == "history" {
            capabilities.push(SubscriberCapability::HistoricalBackfill);
            capabilities.push(SubscriberCapability::DurableReplay);
        } else {
            capabilities.push(SubscriberCapability::Live);
        }
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.registrations
                .lock()
                .expect("registrations")
                .push(self.name);
            Ok(())
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            if let Some(polls) = self.polls.as_ref() {
                polls.fetch_add(1, Ordering::SeqCst);
            }
            let Some(step) = self.steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self.steps.pop_front().expect("peeked step") {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.acknowledgements
                .lock()
                .expect("acknowledgements")
                .push(token.into_bytes());
            Ok(())
        })
    }
}

impl InterestOwnerSubscriber<Ethereum> for ScriptedSubscriber {
    fn replace_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.registrations
                .lock()
                .expect("registrations")
                .push(self.name);
            Ok(())
        })
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.replace_interest_owners(owners)
    }

    fn upsert_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.registrations
                .lock()
                .expect("registrations")
                .push(self.name);
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.registrations
                .lock()
                .expect("registrations")
                .push(self.name);
            Ok(())
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _retained: BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        _owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        Box::pin(async move {
            self.registrations
                .lock()
                .expect("registrations")
                .push(self.name);
            Ok(None)
        })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

fn block_ref(block_number: u64) -> BlockRef {
    BlockRef {
        number: block_number,
        hash: B256::repeat_byte(block_number as u8),
        parent_hash: Some(B256::repeat_byte(block_number.saturating_sub(1) as u8)),
        timestamp: Some(1_700_000_000 + block_number),
    }
}

fn record(block_number: u64) -> ReactiveInputRecord<Ethereum> {
    let block = block_ref(block_number);
    record_for_block(block, B256::repeat_byte(block_number.wrapping_add(1) as u8))
}

fn record_for_block(block: BlockRef, transaction_hash: B256) -> ReactiveInputRecord<Ethereum> {
    ReactiveInputRecord::new(
        ReactiveInput::Log(Log {
            inner: PrimitiveLog::new_unchecked(
                Address::repeat_byte(0xaa),
                vec![B256::repeat_byte(0xbb)],
                Bytes::new(),
            ),
            block_hash: Some(block.hash),
            block_number: Some(block.number),
            block_timestamp: block.timestamp,
            transaction_hash: Some(transaction_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }),
        ReactiveContext {
            chain_id: Some(1),
            source: InputSource::Subscription,
            chain_status: ChainStatus::Included {
                block,
                confirmations: 0,
            },
            block: Some(block),
            transaction_index: Some(0),
            log_index: Some(0),
        },
    )
}

fn record_for_block_with_delta(
    block: BlockRef,
    transaction_hash: B256,
    delta: u8,
) -> ReactiveInputRecord<Ethereum> {
    let mut record = record_for_block(block, transaction_hash);
    let ReactiveInput::Log(log) = &mut record.input else {
        unreachable!("record helper always builds a log")
    };
    log.inner.data.data = Bytes::from(vec![delta]);
    record
}

fn removed_record_for_block(
    mut block: BlockRef,
    transaction_hash: B256,
) -> ReactiveInputRecord<Ethereum> {
    // Alloy log subscriptions ordinarily know the dropped block hash/number
    // but not its parent. Hybrid must recover that predecessor from its exact
    // retained canonical suffix.
    block.parent_hash = None;
    let mut record = record_for_block(block, transaction_hash);
    let ReactiveInput::Log(log) = &mut record.input else {
        unreachable!("record helper always builds a log")
    };
    log.removed = true;
    record.context.chain_status = ChainStatus::Reorged {
        dropped_from: block,
    };
    record.context.block = Some(block);
    record
}

fn replacement_record(
    block_number: u64,
    hash_byte: u8,
    transaction_byte: u8,
) -> ReactiveInputRecord<Ethereum> {
    let mut block = block_ref(block_number);
    block.hash = B256::repeat_byte(hash_byte);
    // Compact log streams may not carry the replacement block's parent either.
    block.parent_hash = None;
    record_for_block(block, B256::repeat_byte(transaction_byte))
}

struct DeltaWriter {
    id: HandlerId,
    slot: U256,
}

impl ReactiveHandler<Ethereum> for DeltaWriter {
    fn id(&self) -> HandlerId {
        self.id.clone()
    }

    fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        vec![portable_log_interest()]
    }

    fn handle(
        &self,
        _ctx: &ReactiveContext,
        input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        let delta = log.inner.data.data.first().copied().unwrap_or(1);
        Ok(HandlerOutcome {
            effects: vec![ReactiveEffect::StateUpdate(StateUpdate::slot_delta(
                Address::repeat_byte(0xaa),
                self.slot,
                SlotDelta::Add(U256::from(delta)),
            ))],
            quality: StateEffectQuality::ExactFromInput,
            tags: Vec::new(),
        })
    }
}

fn record_with_data(block_number: u64, data_len: usize) -> ReactiveInputRecord<Ethereum> {
    let mut record = record(block_number);
    let ReactiveInput::Log(log) = &mut record.input else {
        unreachable!("record helper always builds a log")
    };
    log.inner.data.data = Bytes::from(vec![0x42; data_len]);
    record
}

fn record_with_block_metadata(
    block_number: u64,
    parent_hash: Option<B256>,
    timestamp: Option<u64>,
) -> ReactiveInputRecord<Ethereum> {
    let mut block = block_ref(block_number);
    block.parent_hash = parent_hash;
    block.timestamp = timestamp;
    record_for_block(block, B256::repeat_byte(block_number.wrapping_add(1) as u8))
}

fn header_and_full_block_records(block_number: u64) -> Vec<ReactiveInputRecord<Ethereum>> {
    let block_ref = block_ref(block_number);
    let header = Header {
        hash: block_ref.hash,
        inner: alloy_consensus::Header {
            number: block_ref.number,
            parent_hash: block_ref.parent_hash.expect("test parent"),
            timestamp: block_ref.timestamp.expect("test timestamp"),
            ..Default::default()
        },
        total_difficulty: None,
        size: None,
    };
    let context = ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Subscription,
        chain_status: ChainStatus::Included {
            block: block_ref,
            confirmations: 0,
        },
        block: Some(block_ref),
        transaction_index: None,
        log_index: None,
    };
    vec![
        ReactiveInputRecord::new(ReactiveInput::BlockHeader(header.clone()), context.clone()),
        ReactiveInputRecord::new(ReactiveInput::FullBlock(Block::empty(header)), context),
    ]
}

fn header_record_with_gas_limit(
    block_number: u64,
    gas_limit: u64,
) -> ReactiveInputRecord<Ethereum> {
    let block_ref = block_ref(block_number);
    let header = Header {
        hash: block_ref.hash,
        inner: alloy_consensus::Header {
            number: block_ref.number,
            parent_hash: block_ref.parent_hash.expect("test parent"),
            timestamp: block_ref.timestamp.expect("test timestamp"),
            gas_limit,
            ..Default::default()
        },
        total_difficulty: None,
        size: None,
    };
    let context = ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Subscription,
        chain_status: ChainStatus::Included {
            block: block_ref,
            confirmations: 0,
        },
        block: Some(block_ref),
        transaction_index: None,
        log_index: None,
    };
    ReactiveInputRecord::new(ReactiveInput::BlockHeader(header), context)
}

fn batch(blocks: &[u64], token: &[u8]) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(blocks.iter().copied().map(record).collect())
        .with_delivery_token(SubscriberDeliveryToken::new(token.to_vec()))
}

fn routed_batch(
    blocks: &[u64],
    audience: DeliveryAudience,
    token: &[u8],
) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::from_deliveries(blocks.iter().copied().map(|block| {
        ReactiveInputDelivery::new(record(block), audience.clone(), DeliveryScope::Canonical)
    }))
    .with_delivery_token(SubscriberDeliveryToken::new(token.to_vec()))
}

fn tokenless_batch(blocks: &[u64]) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(blocks.iter().copied().map(record).collect())
}

fn portable_log_interest() -> ReactiveInterest<Ethereum> {
    ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(Address::repeat_byte(0xaa)),
        local_matcher: None,
        route_key: None,
    })
}

fn alternate_log_interest() -> ReactiveInterest<Ethereum> {
    ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(Address::repeat_byte(0xcc)),
        local_matcher: None,
        route_key: None,
    })
}

async fn covered_gap_mutation_hybrid(
    owners: Option<Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>>,
) -> (
    HybridSubscriber<GapMutationSubscriber, GapMutationSubscriber>,
    GapMutationSignals,
) {
    let covered_base = owners.is_none();
    let covered_owners = owners
        .as_ref()
        .into_iter()
        .flatten()
        .filter(|(_, interests)| !interests.is_empty())
        .map(|(owner, _)| owner.clone())
        .collect::<HashSet<_>>();
    let signals = GapMutationSignals {
        fail_historical: Arc::new(AtomicBool::new(false)),
        block_historical: Arc::new(AtomicBool::new(false)),
        injected_events: Arc::new(AtomicUsize::new(0)),
        live_polls: Arc::new(AtomicUsize::new(0)),
        covered_base: Arc::new(AtomicBool::new(false)),
        covered_owners: Arc::new(Mutex::new(HashSet::new())),
    };
    let history = GapMutationSubscriber {
        historical: true,
        steps: VecDeque::from([Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"gap-history-100"),
        )]),
        base_active: false,
        owners: HashMap::new(),
        signals: signals.clone(),
    };
    let live = GapMutationSubscriber {
        historical: false,
        steps: VecDeque::from([Step::Batch(Duration::ZERO, tokenless_batch(&[101]))]),
        base_active: false,
        owners: HashMap::new(),
        signals: signals.clone(),
    };
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("gap coordinator");
    match owners {
        Some(owners) => hybrid
            .upsert_interest_owners(owners)
            .await
            .expect("seed owner topology"),
        None => hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .expect("seed base topology"),
    }
    for _ in 0..2 {
        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
            .await
            .unwrap();
    }
    assert_eq!(hybrid.phase(), HybridPhase::Live);
    assert_eq!(signals.injected_events.load(Ordering::SeqCst), 0);
    signals.covered_base.store(covered_base, Ordering::SeqCst);
    *signals.covered_owners.lock().unwrap() = covered_owners;
    (hybrid, signals)
}

fn resume_position(
    block: u64,
    history: Vec<BlockRef>,
    token: SubscriberDeliveryToken,
    checkpoint: SubscriberCheckpoint,
) -> SubscriberResumePosition {
    SubscriberResumePosition::new(1, block_ref(block), history, Some(token), Some(checkpoint))
}

fn durable_metadata(
    block: u64,
    token: &SubscriberDeliveryToken,
    checkpoint: &SubscriberCheckpoint,
) -> DurableCheckpointMetadata {
    DurableCheckpointMetadata::new(
        DurableCheckpointIdentity::new(1, "hybrid-test", "handlers-v1"),
        DurableCheckpointBlock::new(block, block_ref(block).hash)
            .with_parent_hash(block_ref(block).parent_hash.expect("test parent"))
            .with_timestamp(block_ref(block).timestamp.expect("test timestamp")),
    )
    .with_delivery_token(token.as_bytes().to_vec())
    .with_subscriber_checkpoint(checkpoint.as_bytes().to_vec())
}

fn scoped_batch(blocks: &[u64], owner: HandlerId, token: &[u8]) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::from_scoped_records(
        blocks
            .iter()
            .copied()
            .map(|block| (record(block), DeliveryAudience::Owners(vec![owner.clone()]))),
    )
    .with_delivery_token(SubscriberDeliveryToken::new(token.to_vec()))
}

fn owner_catchup_batch(
    records: impl IntoIterator<Item = ReactiveInputRecord<Ethereum>>,
    owner: HandlerId,
    token: &[u8],
) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::from_deliveries(records.into_iter().map(|record| {
        ReactiveInputDelivery::new(
            record,
            DeliveryAudience::Owners(vec![owner.clone()]),
            DeliveryScope::OwnerCatchup,
        )
    }))
    .with_delivery_token(SubscriberDeliveryToken::new(token.to_vec()))
}

fn coordinated_owner_activation_batch(
    owner: HandlerId,
    retained_anchor: ReactiveInputRecord<Ethereum>,
    canonical_progress: impl IntoIterator<Item = ReactiveInputRecord<Ethereum>>,
    token: &[u8],
) -> ReactiveInputBatch<Ethereum> {
    let deliveries = std::iter::once(ReactiveInputDelivery::new(
        retained_anchor,
        DeliveryAudience::Owners(vec![owner]),
        DeliveryScope::OwnerCatchup,
    ))
    .chain(canonical_progress.into_iter().map(|record| {
        ReactiveInputDelivery::new(
            record,
            DeliveryAudience::All,
            DeliveryScope::CanonicalProgress,
        )
    }));
    ReactiveInputBatch::from_deliveries(deliveries)
        .with_delivery_token(SubscriberDeliveryToken::new(token.to_vec()))
}

fn reorg_batch(
    common_ancestor: BlockRef,
    old_tip: BlockRef,
    new_tip: BlockRef,
    record: ReactiveInputRecord<Ethereum>,
    token: &[u8],
) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![record])
        .with_chain_controls([ChainControl::Reorg {
            common_ancestor,
            old_tip,
            new_tip,
        }])
        .with_delivery_token(SubscriberDeliveryToken::new(token.to_vec()))
}

async fn synthetic_resume_fixture() -> (SubscriberDeliveryToken, SubscriberCheckpoint) {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[104], b"fixture-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[105]))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("fixture coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("fixture register");
    let historical = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(historical.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = hybrid.next_batch().await.unwrap().unwrap();
    let token = live.delivery_token().unwrap().clone();
    let checkpoint = live.subscriber_checkpoint().unwrap().clone();
    hybrid.acknowledge_delivery(token.clone()).await.unwrap();
    (token, checkpoint)
}

struct FailingRegistrationSubscriber {
    results: VecDeque<Result<(), &'static str>>,
}

struct BlockingRegistrationSubscriber {
    name: &'static str,
    registrations: Arc<Mutex<Vec<&'static str>>>,
}

struct CapabilitySubscriber(SubscriberCapabilities);

struct ReplayUntilAcknowledgedSubscriber {
    batch: ReactiveInputBatch<Ethereum>,
    polls: Arc<Mutex<usize>>,
}

struct DurableAckReplaySubscriber {
    historical: bool,
    in_flight: Option<ReactiveInputBatch<Ethereum>>,
    next: VecDeque<ReactiveInputBatch<Ethereum>>,
    acknowledgements: Arc<Mutex<Vec<Vec<u8>>>>,
}

struct DurableLifecycleSubscriber {
    historical: bool,
    steps: VecDeque<Step>,
    polls: Arc<AtomicUsize>,
    restores: Arc<Mutex<Vec<SubscriberResumePosition>>>,
    acknowledgements: Arc<Mutex<Vec<Vec<u8>>>>,
}

struct RestoreProbeSubscriber {
    historical: bool,
    chain_id: Option<u64>,
    restore_results: VecDeque<Result<(), &'static str>>,
    restores: Arc<Mutex<Vec<SubscriberResumePosition>>>,
}

impl RestoreProbeSubscriber {
    fn new(
        historical: bool,
        chain_id: Option<u64>,
        restore_results: impl IntoIterator<Item = Result<(), &'static str>>,
        restores: Arc<Mutex<Vec<SubscriberResumePosition>>>,
    ) -> Self {
        Self {
            historical,
            chain_id,
            restore_results: restore_results.into_iter().collect(),
            restores,
        }
    }
}

impl EventSubscriber<Ethereum> for RestoreProbeSubscriber {
    fn chain_id(&self) -> Option<u64> {
        self.chain_id
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let mut capabilities = vec![
            SubscriberCapability::Logs,
            SubscriberCapability::Barriers,
            SubscriberCapability::DurableReplay,
        ];
        capabilities.push(if self.historical {
            SubscriberCapability::HistoricalBackfill
        } else {
            SubscriberCapability::Live
        });
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(std::future::pending())
    }

    fn restore_position(
        &mut self,
        position: &SubscriberResumePosition,
    ) -> Result<(), SubscriberError> {
        self.restores
            .lock()
            .expect("restore calls")
            .push(position.clone());
        self.restore_results
            .pop_front()
            .unwrap_or(Ok(()))
            .map_err(|message| SubscriberError::Provider(message.into()))
    }
}

impl InterestOwnerSubscriber<Ethereum> for RestoreProbeSubscriber {
    fn replace_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn upsert_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn add_interest_owner(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn remove_interest_owner(
        &mut self,
        _owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        Box::pin(async { Ok(None) })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

fn test_source_capabilities() -> SubscriberCapabilities {
    SubscriberCapabilities::new([
        SubscriberCapability::HistoricalBackfill,
        SubscriberCapability::DurableReplay,
        SubscriberCapability::Barriers,
        SubscriberCapability::Live,
        SubscriberCapability::Logs,
        SubscriberCapability::BlockHeaders,
    ])
}

impl EventSubscriber<Ethereum> for CapabilitySubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        self.0.clone()
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(std::future::pending())
    }
}

impl EventSubscriber<Ethereum> for ReplayUntilAcknowledgedSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        SubscriberCapabilities::new([
            SubscriberCapability::Live,
            SubscriberCapability::Logs,
            SubscriberCapability::Barriers,
        ])
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            *self.polls.lock().expect("poll counter") += 1;
            Ok(Some(self.batch.clone()))
        })
    }
}

impl EventSubscriber<Ethereum> for DurableAckReplaySubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let mut capabilities = vec![
            SubscriberCapability::DurableReplay,
            SubscriberCapability::Logs,
            SubscriberCapability::Barriers,
        ];
        capabilities.push(if self.historical {
            SubscriberCapability::HistoricalBackfill
        } else {
            SubscriberCapability::Live
        });
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            if let Some(in_flight) = self.in_flight.as_ref() {
                return Ok(Some(in_flight.clone()));
            }
            let Some(next) = self.next.pop_front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            self.in_flight = Some(next.clone());
            Ok(Some(next))
        })
    }

    fn restore_position(
        &mut self,
        _position: &SubscriberResumePosition,
    ) -> Result<(), SubscriberError> {
        Ok(())
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.acknowledgements
                .lock()
                .unwrap()
                .push(token.as_bytes().to_vec());
            if self
                .in_flight
                .as_ref()
                .and_then(|batch| batch.delivery_token())
                == Some(&token)
            {
                self.in_flight = None;
            }
            Ok(())
        })
    }
}

impl EventSubscriber<Ethereum> for DurableLifecycleSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let mut capabilities = vec![
            SubscriberCapability::DurableReplay,
            SubscriberCapability::Logs,
            SubscriberCapability::Barriers,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
        ];
        capabilities.push(if self.historical {
            SubscriberCapability::HistoricalBackfill
        } else {
            SubscriberCapability::Live
        });
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            self.polls.fetch_add(1, Ordering::SeqCst);
            let Some(step) = self.steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self
                .steps
                .pop_front()
                .expect("peeked durable lifecycle step")
            {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }

    fn restore_position(
        &mut self,
        position: &SubscriberResumePosition,
    ) -> Result<(), SubscriberError> {
        self.restores.lock().unwrap().push(position.clone());
        Ok(())
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.acknowledgements
                .lock()
                .unwrap()
                .push(token.into_bytes());
            Ok(())
        })
    }
}

impl InterestOwnerSubscriber<Ethereum> for DurableLifecycleSubscriber {
    fn replace_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn upsert_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn add_interest_owner(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        _owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        Box::pin(async { Ok(None) })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

impl EventSubscriber<Ethereum> for BlockingRegistrationSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        test_source_capabilities()
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.registrations
                .lock()
                .expect("registrations")
                .push(self.name);
            std::future::pending().await
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(std::future::pending())
    }
}

impl EventSubscriber<Ethereum> for FailingRegistrationSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        test_source_capabilities()
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            match self.results.pop_front().unwrap_or(Ok(())) {
                Ok(()) => Ok(()),
                Err(message) => Err(SubscriberError::Provider(message.into())),
            }
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(std::future::pending())
    }
}

struct RevisionedHistoricalSubscriber {
    steps: VecDeque<Step>,
    revision: Arc<AtomicU64>,
    mutations: Arc<AtomicUsize>,
    acknowledgements: Arc<Mutex<Vec<Vec<u8>>>>,
    restores: Arc<Mutex<Vec<SubscriberResumePosition>>>,
}

impl RevisionedHistoricalSubscriber {
    fn bump_revision(&self) {
        self.revision.fetch_add(1, Ordering::SeqCst);
        self.mutations.fetch_add(1, Ordering::SeqCst);
    }
}

impl EventSubscriber<Ethereum> for RevisionedHistoricalSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        SubscriberCapabilities::new([
            SubscriberCapability::HistoricalBackfill,
            SubscriberCapability::DurableReplay,
            SubscriberCapability::Barriers,
            SubscriberCapability::Logs,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
        ])
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.bump_revision();
            Ok(())
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            let Some(step) = self.steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self.steps.pop_front().expect("peeked step") {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }

    fn restore_position(
        &mut self,
        position: &SubscriberResumePosition,
    ) -> Result<(), SubscriberError> {
        self.restores
            .lock()
            .expect("restores")
            .push(position.clone());
        let expected = self.revision.load(Ordering::SeqCst).to_be_bytes();
        if position
            .subscriber_checkpoint
            .as_ref()
            .map(SubscriberCheckpoint::as_bytes)
            != Some(expected.as_slice())
        {
            return Err(SubscriberError::Provider(
                "historical desired-state revision does not match restored cursor".into(),
            ));
        }
        Ok(())
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.acknowledgements
                .lock()
                .expect("acknowledgements")
                .push(token.into_bytes());
            Ok(())
        })
    }
}

impl InterestOwnerSubscriber<Ethereum> for RevisionedHistoricalSubscriber {
    fn upsert_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.bump_revision();
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.bump_revision();
            Ok(())
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        _owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        Box::pin(async move {
            self.bump_revision();
            Ok(None)
        })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

struct RegistrationRequiredLiveSubscriber {
    steps: VecDeque<Step>,
    registered: Arc<AtomicBool>,
    mutations: Arc<AtomicUsize>,
    restore_calls: Arc<AtomicUsize>,
}

impl EventSubscriber<Ethereum> for RegistrationRequiredLiveSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        SubscriberCapabilities::new([
            SubscriberCapability::Live,
            SubscriberCapability::Logs,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
        ])
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.registered.store(true, Ordering::SeqCst);
            self.mutations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            if !self.registered.load(Ordering::SeqCst) {
                return Err(SubscriberError::Provider(
                    "ephemeral live source was polled before registration".into(),
                ));
            }
            let Some(step) = self.steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self.steps.pop_front().expect("peeked step") {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }

    fn restore_position(
        &mut self,
        _position: &SubscriberResumePosition,
    ) -> Result<(), SubscriberError> {
        self.restore_calls.fetch_add(1, Ordering::SeqCst);
        Err(SubscriberError::Provider(
            "ephemeral live source must not receive a durable cursor".into(),
        ))
    }
}

impl InterestOwnerSubscriber<Ethereum> for RegistrationRequiredLiveSubscriber {
    fn upsert_interest_owners(
        &mut self,
        _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.registered.store(true, Ordering::SeqCst);
            self.mutations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        self.upsert_interest_owners(Vec::new())
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        _owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        Box::pin(async { Ok(None) })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

struct LifecycleStateSubscriber {
    historical: bool,
    steps: VecDeque<Step>,
    owner_installed: Arc<AtomicBool>,
    removals: Arc<AtomicUsize>,
    block_first_add_after_commit: bool,
}

#[derive(Clone)]
struct GapMutationSignals {
    fail_historical: Arc<AtomicBool>,
    block_historical: Arc<AtomicBool>,
    injected_events: Arc<AtomicUsize>,
    live_polls: Arc<AtomicUsize>,
    covered_base: Arc<AtomicBool>,
    covered_owners: Arc<Mutex<HashSet<HandlerId>>>,
}

struct GapMutationSubscriber {
    historical: bool,
    steps: VecDeque<Step>,
    base_active: bool,
    owners: HashMap<HandlerId, bool>,
    signals: GapMutationSignals,
}

impl GapMutationSubscriber {
    fn note_live_gap(&self, covered_active: bool) {
        if !self.historical && covered_active {
            let _ = self.signals.injected_events.compare_exchange(
                0,
                1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
    }

    async fn finish_mutation(&mut self) -> Result<(), SubscriberError> {
        if !self.historical {
            return Ok(());
        }
        if self.signals.block_historical.swap(false, Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.signals.fail_historical.swap(false, Ordering::SeqCst) {
            return Err(SubscriberError::Provider(
                "historical lifecycle commit failed after live mutation".into(),
            ));
        }
        Ok(())
    }
}

impl EventSubscriber<Ethereum> for GapMutationSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let mut capabilities = vec![
            SubscriberCapability::Logs,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
            SubscriberCapability::Barriers,
        ];
        if self.historical {
            capabilities.extend([
                SubscriberCapability::HistoricalBackfill,
                SubscriberCapability::DurableReplay,
            ]);
        } else {
            capabilities.push(SubscriberCapability::Live);
        }
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        let next_active = !interests.is_empty();
        Box::pin(async move {
            self.note_live_gap(self.signals.covered_base.load(Ordering::SeqCst));
            self.base_active = next_active;
            self.finish_mutation().await
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            if !self.historical {
                self.signals.live_polls.fetch_add(1, Ordering::SeqCst);
            }
            let Some(step) = self.steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self.steps.pop_front().expect("peeked lifecycle-gap step") {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }
}

impl InterestOwnerSubscriber<Ethereum> for GapMutationSubscriber {
    fn replace_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.note_live_gap(!self.signals.covered_owners.lock().unwrap().is_empty());
            self.base_active = false;
            self.owners = owners
                .into_iter()
                .map(|(owner, interests)| (owner, !interests.is_empty()))
                .collect();
            self.finish_mutation().await
        })
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.replace_interest_owners(owners)
    }

    fn upsert_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            let changed_active = {
                let covered = self.signals.covered_owners.lock().unwrap();
                owners.iter().any(|(owner, _)| covered.contains(owner))
            };
            self.note_live_gap(changed_active);
            for (owner, interests) in owners {
                self.owners.insert(owner, !interests.is_empty());
            }
            self.finish_mutation().await
        })
    }

    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        let next_active = !interests.is_empty();
        Box::pin(async move {
            self.note_live_gap(self.signals.covered_owners.lock().unwrap().contains(&owner));
            self.owners.insert(owner, next_active);
            self.finish_mutation().await
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _retained: BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        let owner = owner.clone();
        Box::pin(async move {
            let previous_active = self.owners.remove(&owner).unwrap_or(false);
            self.note_live_gap(self.signals.covered_owners.lock().unwrap().contains(&owner));
            self.finish_mutation().await?;
            Ok(previous_active.then(Vec::new))
        })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

impl EventSubscriber<Ethereum> for LifecycleStateSubscriber {
    fn chain_id(&self) -> Option<u64> {
        Some(1)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let mut capabilities = vec![
            SubscriberCapability::Logs,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
        ];
        if self.historical {
            capabilities.extend([
                SubscriberCapability::HistoricalBackfill,
                SubscriberCapability::DurableReplay,
                SubscriberCapability::Barriers,
            ]);
        } else {
            capabilities.push(SubscriberCapability::Live);
        }
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            let Some(step) = self.steps.front() else {
                std::future::pending::<()>().await;
                unreachable!();
            };
            let delay = match step {
                Step::Batch(delay, _) | Step::Error(delay, _) | Step::End(delay) => *delay,
            };
            tokio::time::sleep(delay).await;
            match self.steps.pop_front().expect("peeked lifecycle step") {
                Step::Batch(_, batch) => Ok(Some(batch)),
                Step::Error(_, message) => Err(SubscriberError::Provider(message.into())),
                Step::End(_) => Ok(None),
            }
        })
    }
}

impl InterestOwnerSubscriber<Ethereum> for LifecycleStateSubscriber {
    fn upsert_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.owner_installed
                .store(!owners.is_empty(), Ordering::SeqCst);
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        _owner: HandlerId,
        _interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.owner_installed.store(true, Ordering::SeqCst);
            if self.block_first_add_after_commit {
                self.block_first_add_after_commit = false;
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        _retained: BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        self.add_interest_owner(owner, interests)
    }

    fn remove_interest_owner(
        &mut self,
        _owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        Box::pin(async move {
            self.owner_installed.store(false, Ordering::SeqCst);
            self.removals.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        })
    }

    fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        None
    }
}

async fn revisioned_checkpoint_fixture() -> (
    HybridSubscriber<RevisionedHistoricalSubscriber, RegistrationRequiredLiveSubscriber>,
    SubscriberResumePosition,
    DurableCheckpointMetadata,
    ReactiveInterest<Ethereum>,
    Arc<AtomicU64>,
    Arc<AtomicUsize>,
) {
    let revision = Arc::new(AtomicU64::new(0));
    let historical_mutations = Arc::new(AtomicUsize::new(0));
    let history = RevisionedHistoricalSubscriber {
        steps: VecDeque::from([Step::Batch(
            Duration::from_millis(10),
            batch(&[104], b"history-104")
                .with_subscriber_checkpoint(SubscriberCheckpoint::new(1u64.to_be_bytes().to_vec())),
        )]),
        revision: Arc::clone(&revision),
        mutations: Arc::clone(&historical_mutations),
        acknowledgements: Arc::new(Mutex::new(Vec::new())),
        restores: Arc::new(Mutex::new(Vec::new())),
    };
    let live = RegistrationRequiredLiveSubscriber {
        steps: VecDeque::from([Step::Batch(Duration::ZERO, tokenless_batch(&[105]))]),
        registered: Arc::new(AtomicBool::new(false)),
        mutations: Arc::new(AtomicUsize::new(0)),
        restore_calls: Arc::new(AtomicUsize::new(0)),
    };
    let interest = portable_log_interest();
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(std::slice::from_ref(&interest))
        .await
        .unwrap();
    let historical = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(historical.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = hybrid.next_batch().await.unwrap().unwrap();
    let token = live.delivery_token().unwrap().clone();
    let checkpoint = live.subscriber_checkpoint().unwrap().clone();
    hybrid.acknowledge_delivery(token.clone()).await.unwrap();
    let position = resume_position(105, vec![block_ref(105)], token.clone(), checkpoint.clone());
    let metadata = durable_metadata(105, &token, &checkpoint);
    (
        hybrid,
        position,
        metadata,
        interest,
        revision,
        historical_mutations,
    )
}

#[tokio::test]
async fn live_first_registration_and_ack_gated_cutover() {
    let history_acks = Arc::new(Mutex::new(Vec::new()));
    let live_acks = Arc::new(Mutex::new(Vec::new()));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [
            Step::Batch(Duration::from_millis(20), batch(&[104], b"h104")),
            Step::End(Duration::from_secs(1)),
        ],
        Arc::clone(&history_acks),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[105], b"l105")),
            Step::Batch(Duration::from_millis(100), batch(&[106], b"l106")),
        ],
        Arc::clone(&live_acks),
        Arc::clone(&registrations),
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");
    assert_eq!(
        *registrations.lock().expect("registrations"),
        vec!["live", "history"],
        "head source must register before catch-up source"
    );

    let historical = hybrid
        .next_batch()
        .await
        .expect("historical delivery")
        .expect("historical batch");
    assert_eq!(hybrid.phase(), HybridPhase::CatchingUp);
    assert_eq!(hybrid.fence(), Some(104));
    assert_eq!(hybrid.buffered_live_batches(), 1);
    assert_eq!(
        historical.records()[0]
            .context
            .block
            .as_ref()
            .unwrap()
            .number,
        104
    );

    let cutover_token = historical.delivery_token().expect("wrapped token").clone();
    hybrid
        .acknowledge_delivery(cutover_token)
        .await
        .expect("ack historical fence");
    assert_eq!(hybrid.phase(), HybridPhase::DrainingLive);
    assert_eq!(
        *history_acks.lock().expect("history acks"),
        vec![b"h104".to_vec()]
    );

    let buffered_live = hybrid
        .next_batch()
        .await
        .expect("buffered live")
        .expect("live batch");
    assert_eq!(
        buffered_live.records()[0]
            .context
            .block
            .as_ref()
            .unwrap()
            .number,
        105
    );
    let live_token = buffered_live.delivery_token().expect("live token").clone();
    hybrid
        .acknowledge_delivery(live_token)
        .await
        .expect("ack live batch");
    assert_eq!(
        *live_acks.lock().expect("live acks"),
        vec![b"l105".to_vec()]
    );

    let next_live = hybrid
        .next_batch()
        .await
        .expect("next live")
        .expect("next live batch");
    assert_eq!(hybrid.phase(), HybridPhase::Live);
    assert_eq!(
        next_live.records()[0]
            .context
            .block
            .as_ref()
            .unwrap()
            .number,
        106
    );
}

#[tokio::test]
async fn historical_end_is_not_treated_as_fence_coverage() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::End(Duration::from_millis(10))],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, batch(&[105], b"live"))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let error = hybrid
        .next_batch()
        .await
        .expect_err("premature history end");
    assert!(
        error
            .to_string()
            .contains("without an acknowledged coverage proof")
    );
    assert_eq!(hybrid.phase(), HybridPhase::CatchingUp);
}

#[tokio::test]
async fn acknowledged_history_before_the_first_live_fence_cuts_over_when_the_fence_arrives() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::ZERO, batch(&[100], b"history-100"))],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::from_millis(20),
                tokenless_batch(&[101]),
            )],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();

        let historical = hybrid.next_batch().await.unwrap().unwrap();
        assert_eq!(
            historical.records()[0]
                .context
                .block
                .as_ref()
                .unwrap()
                .number,
            100
        );
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        assert_eq!(hybrid.phase(), HybridPhase::CatchingUp);
        assert_eq!(hybrid.fence(), None);

        let live = hybrid.next_batch().await.unwrap().unwrap();
        assert_eq!(
            live.records()[0].context.block.as_ref().unwrap().number,
            101
        );
        hybrid
            .acknowledge_delivery(live.delivery_token().unwrap().clone())
            .await
            .unwrap();
        assert_eq!(hybrid.phase(), HybridPhase::Live);
    })
    .await
    .expect("late live-fence cutover regression timed out");
}

#[tokio::test]
async fn acknowledged_history_proof_survives_restart_before_a_late_live_fence() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let interest = portable_log_interest();
        let revision = Arc::new(AtomicU64::new(0));
        let equal_progress = ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::CanonicalProgress(block_ref(104))])
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(1u64.to_be_bytes().to_vec()))
            .with_delivery_token(SubscriberDeliveryToken::new(
                b"equal-history-before-crash".to_vec(),
            ));
        let history = RevisionedHistoricalSubscriber {
            steps: VecDeque::from([
                Step::Batch(
                    Duration::ZERO,
                    batch(&[104], b"history-seed").with_subscriber_checkpoint(
                        SubscriberCheckpoint::new(1u64.to_be_bytes().to_vec()),
                    ),
                ),
                Step::Batch(Duration::ZERO, equal_progress),
            ]),
            revision: Arc::clone(&revision),
            mutations: Arc::new(AtomicUsize::new(0)),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
            restores: Arc::new(Mutex::new(Vec::new())),
        };
        let live = RegistrationRequiredLiveSubscriber {
            steps: VecDeque::from([Step::Batch(
                Duration::from_millis(100),
                tokenless_batch(&[105]),
            )]),
            registered: Arc::new(AtomicBool::new(false)),
            mutations: Arc::new(AtomicUsize::new(0)),
            restore_calls: Arc::new(AtomicUsize::new(0)),
        };
        let mut before_crash =
            HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        before_crash
            .register_interests(std::slice::from_ref(&interest))
            .await
            .unwrap();
        let seed = before_crash.next_batch().await.unwrap().unwrap();
        before_crash
            .acknowledge_delivery(seed.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let historical = before_crash.next_batch().await.unwrap().unwrap();
        assert!(historical.records().is_empty());
        assert!(historical.chain_controls().is_empty());
        let token = historical.delivery_token().unwrap().clone();
        let checkpoint = historical.subscriber_checkpoint().unwrap().clone();
        before_crash
            .acknowledge_delivery(token.clone())
            .await
            .unwrap();
        assert_eq!(before_crash.fence(), None);
        let position = resume_position(104, vec![block_ref(104)], token, checkpoint);
        drop(before_crash);

        let history = RevisionedHistoricalSubscriber {
            steps: VecDeque::new(),
            revision,
            mutations: Arc::new(AtomicUsize::new(0)),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
            restores: Arc::new(Mutex::new(Vec::new())),
        };
        let live = RegistrationRequiredLiveSubscriber {
            steps: VecDeque::from([Step::Batch(Duration::ZERO, tokenless_batch(&[105]))]),
            registered: Arc::new(AtomicBool::new(false)),
            mutations: Arc::new(AtomicUsize::new(0)),
            restore_calls: Arc::new(AtomicUsize::new(0)),
        };
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        restored
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();

        let live = restored.next_batch().await.unwrap().unwrap();
        assert_eq!(
            live.records()[0].context.block.as_ref().unwrap().number,
            105
        );
        assert_eq!(restored.phase(), HybridPhase::DrainingLive);
    })
    .await
    .expect("restart-before-late-fence regression timed out");
}

#[tokio::test]
async fn canonical_progress_control_proves_a_head_only_cutover() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let progress = ReactiveInputBatch::new(Vec::new())
        .with_chain_id(1)
        .with_chain_controls([ChainControl::CanonicalProgress(block_ref(104))])
        .with_delivery_token(SubscriberDeliveryToken::new(b"progress".to_vec()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::from_millis(10), progress)],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, batch(&[105], b"live"))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let progress = hybrid.next_batch().await.unwrap().unwrap();
    assert!(progress.records().is_empty());
    assert!(matches!(
        progress.chain_controls(),
        [ChainControl::CanonicalProgress(block)] if block.number == 104
    ));
    hybrid
        .acknowledge_delivery(progress.delivery_token().unwrap().clone())
        .await
        .unwrap();
    assert_eq!(hybrid.phase(), HybridPhase::DrainingLive);
    let live = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(
        live.records()[0].context.block.as_ref().unwrap().number,
        105
    );
}

#[tokio::test]
async fn canonical_progress_commits_a_zero_log_tail_after_sparse_records() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let certified_head = block_ref(104);
    let history_page = ReactiveInputBatch::new(vec![record(100)])
        .with_chain_controls([ChainControl::CanonicalProgress(certified_head)])
        .with_delivery_token(SubscriberDeliveryToken::new(
            b"history-through-104".to_vec(),
        ));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::from_millis(10), history_page)],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[105]))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let delivery = hybrid.next_batch().await.unwrap().unwrap();
    let token = delivery.delivery_token().unwrap().clone();
    let checkpoint = delivery.subscriber_checkpoint().unwrap().clone();
    hybrid.acknowledge_delivery(token.clone()).await.unwrap();

    let historical_restores = Arc::new(Mutex::new(Vec::new()));
    let mut restored = HybridSubscriber::new(
        RestoreProbeSubscriber::new(true, Some(1), [Ok(())], Arc::clone(&historical_restores)),
        RestoreProbeSubscriber::new(false, Some(1), [], Arc::new(Mutex::new(Vec::new()))),
        HybridConfig::default(),
    )
    .unwrap();
    let position = resume_position(104, vec![block_ref(100), certified_head], token, checkpoint);
    restored
        .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
        .await
        .unwrap();
    restored.restore_position(&position).unwrap();

    let historical = historical_restores.lock().unwrap();
    let restored_source = historical.last().expect("historical source restore");
    assert_eq!(restored_source.coverage_head, certified_head);
    assert_eq!(
        restored_source.canonical_history.last(),
        Some(&certified_head)
    );
}

#[tokio::test]
async fn post_record_progress_enriches_same_head_metadata_in_the_checkpoint() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let complete = block_ref(100);
    let partial = record_with_block_metadata(100, None, None);
    let history_page = ReactiveInputBatch::new(vec![partial])
        .with_chain_controls([ChainControl::CanonicalProgress(complete)])
        .with_delivery_token(SubscriberDeliveryToken::new(b"enriched-100".to_vec()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::from_millis(10), history_page)],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[101]))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let delivery = hybrid.next_batch().await.unwrap().unwrap();
    let token = delivery.delivery_token().unwrap().clone();
    let checkpoint = delivery.subscriber_checkpoint().unwrap().clone();
    hybrid.acknowledge_delivery(token.clone()).await.unwrap();

    let historical_restores = Arc::new(Mutex::new(Vec::new()));
    let mut restored = HybridSubscriber::new(
        RestoreProbeSubscriber::new(true, Some(1), [Ok(())], Arc::clone(&historical_restores)),
        RestoreProbeSubscriber::new(false, Some(1), [], Arc::new(Mutex::new(Vec::new()))),
        HybridConfig::default(),
    )
    .unwrap();
    let position = resume_position(100, vec![complete], token, checkpoint);
    restored
        .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
        .await
        .unwrap();
    restored.restore_position(&position).unwrap();

    let historical = historical_restores.lock().unwrap();
    let restored_source = historical.last().expect("historical source restore");
    assert_eq!(restored_source.coverage_head, complete);
    assert_eq!(
        restored_source.coverage_head.parent_hash,
        complete.parent_hash
    );
    assert_eq!(restored_source.coverage_head.timestamp, complete.timestamp);
}

#[tokio::test]
async fn owner_backfill_added_while_live_reenters_ack_gated_catchup() {
    let history_acks = Arc::new(Mutex::new(Vec::new()));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [
            Step::Batch(Duration::from_millis(20), batch(&[104], b"h104")),
            Step::Batch(Duration::from_millis(20), batch(&[109], b"h109")),
        ],
        Arc::clone(&history_acks),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[105], b"l105")),
            Step::Batch(Duration::from_millis(100), batch(&[106], b"l106")),
            Step::Batch(Duration::ZERO, batch(&[110], b"l110")),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .upsert_interest_owners(vec![(
            HandlerId::new("seed-owner"),
            vec![portable_log_interest()],
        )])
        .await
        .expect("register owner topology");

    let initial_history = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(initial_history.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let initial_live = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(initial_live.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live_head = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(live_head.delivery_token().unwrap().clone())
        .await
        .unwrap();
    hybrid
        .add_interest_owner_with_backfill(
            HandlerId::new("dynamic-owner"),
            &[portable_log_interest()],
            SubscriberBackfill::from_block(100),
        )
        .await
        .expect("add dynamic owner");
    assert_eq!(hybrid.phase(), HybridPhase::CatchingUp);

    let dynamic_history = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(
        dynamic_history.records()[0]
            .context
            .block
            .as_ref()
            .unwrap()
            .number,
        109,
        "the historical source must deliver the scoped backfill before live resumes"
    );
    hybrid
        .acknowledge_delivery(dynamic_history.delivery_token().unwrap().clone())
        .await
        .unwrap();
    assert_eq!(hybrid.phase(), HybridPhase::DrainingLive);
    assert_eq!(
        *history_acks.lock().expect("history acks"),
        vec![b"h104".to_vec(), b"h109".to_vec()]
    );
}

#[tokio::test]
async fn owner_catchup_ahead_of_committed_canonical_overlap_fails_before_delivery_or_ack() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let owner = HandlerId::new("dynamic-owner");
        let history_acks = Arc::new(Mutex::new(Vec::new()));
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [
                Step::Batch(Duration::from_millis(20), batch(&[100], b"history-100")),
                Step::Batch(
                    Duration::ZERO,
                    owner_catchup_batch([record(103)], owner.clone(), b"owner-103"),
                ),
            ],
            Arc::clone(&history_acks),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                Step::Batch(Duration::from_millis(30), batch(&[102], b"live-102")),
                Step::Batch(Duration::from_millis(50), batch(&[104], b"live-104")),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .upsert_interest_owners(vec![(
                HandlerId::new("seed-owner"),
                vec![portable_log_interest()],
            )])
            .await
            .unwrap();
        for _ in 0..3 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        assert_eq!(hybrid.phase(), HybridPhase::Live);
        hybrid
            .add_interest_owner_with_backfill(
                owner,
                &[portable_log_interest()],
                SubscriberBackfill::from_canonical_block(block_ref(102)),
            )
            .await
            .unwrap();

        let error = hybrid
            .next_batch()
            .await
            .expect_err("owner-only input cannot outrun canonical rollback coverage");
        assert!(
            error
                .to_string()
                .contains("not an exact retained canonical overlap")
        );
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
        assert_eq!(
            *history_acks.lock().unwrap(),
            vec![b"history-100".to_vec()],
            "the unsafe owner-only page must remain unacknowledged"
        );
    })
    .await
    .expect("owner-catchup overlap gate timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn owner_activation_catches_live_advance_and_later_reorg_rolls_back_both_handlers() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let old_owner = HandlerId::new("old-owner");
        let new_owner = HandlerId::new("new-owner");
        let old_slot = U256::from(1);
        let new_slot = U256::from(2);

        let old_100 =
            record_for_block_with_delta(block_ref(100), B256::repeat_byte(0x10), 1);
        let old_101 =
            record_for_block_with_delta(block_ref(101), B256::repeat_byte(0x11), 1);
        let old_102 =
            record_for_block_with_delta(block_ref(102), B256::repeat_byte(0x12), 1);
        let old_103 =
            record_for_block_with_delta(block_ref(103), B256::repeat_byte(0x13), 2);
        let old_104 =
            record_for_block_with_delta(block_ref(104), B256::repeat_byte(0x14), 3);

        let branch_b_103 = BlockRef {
            number: 103,
            hash: B256::repeat_byte(0xe3),
            parent_hash: Some(block_ref(102).hash),
            timestamp: block_ref(103).timestamp,
        };
        let branch_b_104 = BlockRef {
            number: 104,
            hash: B256::repeat_byte(0xe4),
            parent_hash: Some(branch_b_103.hash),
            timestamp: block_ref(104).timestamp,
        };
        let replacement_103 =
            record_for_block_with_delta(branch_b_103, B256::repeat_byte(0xb3), 10);
        let replacement_104 =
            record_for_block_with_delta(branch_b_104, B256::repeat_byte(0xb4), 20);

        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [
                Step::Batch(
                    Duration::from_millis(20),
                    ReactiveInputBatch::new(vec![old_100, old_101])
                        .with_delivery_token(SubscriberDeliveryToken::new(b"history-101".to_vec())),
                ),
                Step::Batch(
                    Duration::from_millis(30),
                    coordinated_owner_activation_batch(
                        new_owner.clone(),
                        old_102.clone(),
                        [old_103.clone(), old_104.clone()],
                        b"owner-activation-through-104",
                    ),
                ),
            ],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let reorg = ReactiveInputBatch::from_deliveries([
            ReactiveInputDelivery::new(
                replacement_103,
                DeliveryAudience::All,
                DeliveryScope::CanonicalProgress,
            ),
            ReactiveInputDelivery::new(
                replacement_104,
                DeliveryAudience::All,
                DeliveryScope::CanonicalProgress,
            ),
        ])
        .with_chain_controls([ChainControl::Reorg {
            common_ancestor: block_ref(102),
            old_tip: block_ref(104),
            new_tip: branch_b_104,
        }])
        .with_delivery_token(SubscriberDeliveryToken::new(b"branch-b".to_vec()));
        let live = RegistrationGatedLiveSubscriber::new(
            ReactiveInputBatch::new(vec![old_102.clone()]),
            [
                Step::Batch(
                    Duration::from_millis(40),
                    ReactiveInputBatch::new(vec![old_103.clone()]),
                ),
                Step::Batch(
                    Duration::ZERO,
                    ReactiveInputBatch::new(vec![old_104.clone()]),
                ),
                Step::Batch(Duration::from_millis(80), reorg),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .upsert_interest_owners(vec![(
                old_owner.clone(),
                vec![portable_log_interest()],
            )])
            .await
            .unwrap();

        let provider = ProviderBuilder::new()
            .network::<AnyNetwork>()
            .connect_mocked_client(Asserter::new());
        let mut cache = EvmCache::new(Arc::new(provider)).await;
        cache.apply_updates(&[
            StateUpdate::slot(Address::repeat_byte(0xaa), old_slot, U256::ZERO),
            StateUpdate::slot(Address::repeat_byte(0xaa), new_slot, U256::ZERO),
        ]);
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        runtime
            .register_handler(Arc::new(DeltaWriter {
                id: old_owner.clone(),
                slot: old_slot,
            }))
            .unwrap();
        let mut engine = ReactiveEngine::new(runtime, hybrid);

        for expected_head in [101, 102] {
            let delivery = tokio::time::timeout(
                Duration::from_millis(500),
                engine.subscriber_mut().next_batch(),
            )
            .await
            .expect("initial owner delivery timed out")
            .unwrap()
            .unwrap();
            let token = delivery.delivery_token().unwrap().clone();
            engine
                .runtime_mut()
                .ingest_batch(&mut cache, delivery)
                .unwrap();
            engine
                .subscriber_mut()
                .acknowledge_delivery(token)
                .await
                .unwrap();
            assert_eq!(
                engine
                    .runtime()
                    .last_canonical_block()
                    .map(|block| block.number),
                Some(expected_head)
            );
        }
        assert_eq!(
            cache.cached_storage_value(Address::repeat_byte(0xaa), old_slot),
            Some(U256::from(3))
        );

        engine
            .register_handler(Arc::new(DeltaWriter {
                id: new_owner.clone(),
                slot: new_slot,
            }))
            .await
            .unwrap();

        let catchup = tokio::time::timeout(
            Duration::from_millis(500),
            engine.subscriber_mut().next_batch(),
        )
        .await
        .expect("coordinated owner catch-up timed out")
        .unwrap()
        .unwrap();
        assert_eq!(catchup.records().len(), 3);
        assert_eq!(
            catchup.record_delivery_scope(0),
            Some(DeliveryScope::OwnerCatchup)
        );
        assert_eq!(
            catchup.record_delivery_scope(1),
            Some(DeliveryScope::CanonicalProgress)
        );
        assert_eq!(
            catchup.record_delivery_scope(2),
            Some(DeliveryScope::CanonicalProgress)
        );
        let token = catchup.delivery_token().unwrap().clone();
        engine
            .runtime_mut()
            .ingest_batch(&mut cache, catchup)
            .unwrap();
        engine
            .subscriber_mut()
            .acknowledge_delivery(token)
            .await
            .unwrap();
        assert!(engine.runtime().has_journaled_handler_effects(&new_owner));
        assert_eq!(
            cache.cached_storage_value(Address::repeat_byte(0xaa), old_slot),
            Some(U256::from(8))
        );
        assert_eq!(
            cache.cached_storage_value(Address::repeat_byte(0xaa), new_slot),
            Some(U256::from(6))
        );

        let branch_b = tokio::time::timeout(
            Duration::from_millis(500),
            engine.subscriber_mut().next_batch(),
        )
        .await
        .expect("branch-B reorg delivery timed out")
        .unwrap()
        .unwrap();
        assert!(matches!(
            branch_b.chain_controls(),
            [ChainControl::Reorg { common_ancestor, old_tip, new_tip }]
                if common_ancestor == &block_ref(102)
                    && old_tip == &block_ref(104)
                    && new_tip == &branch_b_104
        ));
        let token = branch_b.delivery_token().unwrap().clone();
        engine
            .runtime_mut()
            .ingest_batch(&mut cache, branch_b)
            .unwrap();
        engine
            .subscriber_mut()
            .acknowledge_delivery(token)
            .await
            .unwrap();

        assert_eq!(engine.runtime().last_canonical_block(), Some(branch_b_104));
        assert_eq!(
            cache.cached_storage_value(Address::repeat_byte(0xaa), old_slot),
            Some(U256::from(33)),
            "the old handler's +2/+3 branch-A effects must roll back before +10/+20"
        );
        assert_eq!(
            cache.cached_storage_value(Address::repeat_byte(0xaa), new_slot),
            Some(U256::from(31)),
            "the new handler's retained +1 anchor remains, while its +2/+3 branch-A effects roll back before +10/+20"
        );
    })
    .await
    .expect("coordinated owner activation/reorg regression timed out");
}

#[tokio::test]
async fn tokened_finality_only_live_batch_fails_fast_instead_of_deadlocking() {
    let history_acks = Arc::new(Mutex::new(Vec::new()));
    let live_acks = Arc::new(Mutex::new(Vec::new()));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(20),
            batch(&[99], b"history-99"),
        )],
        Arc::clone(&history_acks),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(
            Duration::ZERO,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Safe(block_ref(100))])
                .with_delivery_token(SubscriberDeliveryToken::new(b"safe-100".to_vec())),
        )],
        live_acks,
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let error = hybrid
        .next_batch()
        .await
        .expect_err("tokened blockless live delivery is unsupported while buffered");
    assert!(error.to_string().contains("without canonical coverage"));
    assert_eq!(hybrid.fence(), None);
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn safe_and_finalized_records_prove_event_coverage_for_cutover() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let mut historical_record = record(100);
        historical_record.context.chain_status = ChainStatus::Safe {
            block: block_ref(100),
        };
        let history_batch = ReactiveInputBatch::new(vec![historical_record])
            .with_delivery_token(SubscriberDeliveryToken::new(b"history-safe".to_vec()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::from_millis(20), history_batch)],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let mut live_record = record(101);
        live_record.context.chain_status = ChainStatus::Finalized {
            block: block_ref(101),
        };
        let live_batch = ReactiveInputBatch::new(vec![live_record])
            .with_delivery_token(SubscriberDeliveryToken::new(b"live-finalized".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, live_batch)],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();

        let historical = hybrid.next_batch().await.unwrap().unwrap();
        assert!(matches!(
            historical.records()[0].context.chain_status,
            ChainStatus::Safe { .. }
        ));
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let live = hybrid.next_batch().await.unwrap().unwrap();
        assert!(matches!(
            live.records()[0].context.chain_status,
            ChainStatus::Finalized { .. }
        ));
        hybrid
            .acknowledge_delivery(live.delivery_token().unwrap().clone())
            .await
            .unwrap();
        assert_eq!(hybrid.phase(), HybridPhase::Live);
    })
    .await
    .expect("safe/finalized event-coverage regression timed out");
}

#[tokio::test]
async fn live_failure_during_initial_catchup_enters_recovery() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_secs(1),
            batch(&[99], b"history-99"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Error(Duration::ZERO, "live disconnected")],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let error = hybrid.next_batch().await.expect_err("live failure");
    assert!(error.to_string().contains("live disconnected"));
    assert_eq!(hybrid.phase(), HybridPhase::Recovering);
}

#[tokio::test]
async fn live_buffer_byte_bound_accounts_for_control_and_checkpoint_bytes() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_secs(1),
            batch(&[99], b"history-99"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(
            Duration::ZERO,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([ChainControl::Barrier {
                    id: vec![0x42; 2_048],
                    block: Some(block_ref(100)),
                }])
                .with_delivery_token(SubscriberDeliveryToken::new(vec![0x11; 128]))
                .with_subscriber_checkpoint(SubscriberCheckpoint::new(vec![0x22; 128])),
        )],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_buffered_live_bytes = 1_024;
    let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let error = hybrid
        .next_batch()
        .await
        .expect_err("oversized control batch");
    assert!(error.to_string().contains("per-batch ingress byte bound"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
#[allow(clippy::field_reassign_with_default)] // Public HybridConfig is non-exhaustive.
async fn opaque_child_cursor_bounds_are_enforced_on_ingress() {
    for oversized_checkpoint in [false, true] {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_secs(1),
                batch(&[99], b"cursor-bound-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let mut live_batch = ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Barrier {
                id: b"cursor-bound-live".to_vec(),
                block: Some(block_ref(100)),
            }])
            .with_delivery_token(SubscriberDeliveryToken::new(vec![
                0x11;
                if oversized_checkpoint {
                    4
                } else {
                    5
                }
            ]));
        if oversized_checkpoint {
            live_batch =
                live_batch.with_subscriber_checkpoint(SubscriberCheckpoint::new(vec![0x22; 5]));
        }
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, live_batch)],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut config = HybridConfig::default();
        config.max_source_delivery_token_bytes = 4;
        config.max_source_checkpoint_bytes = 4;
        let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .expect("register");

        let error = hybrid
            .next_batch()
            .await
            .expect_err("oversized opaque child cursor");

        assert!(error.to_string().contains("configured opaque cursor bound"));
        assert_eq!(hybrid.buffered_live_batches(), 0);
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    }
}

#[tokio::test]
async fn child_control_count_is_rejected_before_control_traversal() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_secs(1),
            batch(&[99], b"control-count-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let controls = (0..4)
        .map(|index| ChainControl::Barrier {
            id: vec![index],
            block: Some(block_ref(100 + u64::from(index))),
        })
        .collect::<Vec<_>>();
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(
            Duration::ZERO,
            ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls(controls),
        )],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_buffered_live_bytes = 1_024;
    let mut hybrid = HybridSubscriber::new(history, live, config).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let error = hybrid
        .next_batch()
        .await
        .expect_err("derived control bound");
    assert!(error.to_string().contains("ingress control bound"));
    assert_eq!(hybrid.buffered_live_batches(), 0);
}

#[tokio::test]
async fn cumulative_live_buffer_byte_bound_applies_after_per_batch_preflight() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(200),
            batch(&[99], b"history-99"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [100_u64, 101].map(|number| {
            Step::Batch(
                Duration::ZERO,
                ReactiveInputBatch::new(Vec::new())
                    .with_chain_id(1)
                    .with_chain_controls([ChainControl::Barrier {
                        id: vec![0x42; 400],
                        block: Some(block_ref(number)),
                    }]),
            )
        }),
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_buffered_live_bytes = 1_500;
    let mut hybrid = HybridSubscriber::new(history, live, config).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let error = hybrid.next_batch().await.unwrap_err();
    assert!(error.to_string().contains("live buffer exceeded"));
    assert!(error.to_string().contains("accounted bytes"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn bulk_owner_bootstrap_commits_once_per_source() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    let owner_a = HandlerId::new("owner-a");
    let owner_b = HandlerId::new("owner-b");
    hybrid
        .upsert_interest_owners(vec![
            (owner_a.clone(), Vec::new()),
            (owner_b.clone(), Vec::new()),
        ])
        .await
        .expect("bulk owner bootstrap");

    assert_eq!(
        *registrations.lock().expect("registrations"),
        vec!["live", "history"]
    );
    assert!(
        hybrid
            .owner_interests(&owner_a)
            .is_some_and(<[ReactiveInterest<Ethereum>]>::is_empty)
    );
    assert!(
        hybrid
            .owner_interests(&owner_b)
            .is_some_and(<[ReactiveInterest<Ethereum>]>::is_empty)
    );
}

#[tokio::test]
async fn post_coverage_owner_replacement_removes_stale_owners_and_commits_once_per_source() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"replacement-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[101]))],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    let stale = HandlerId::new("stale");
    let retained = HandlerId::new("retained");
    let added = HandlerId::new("added");
    hybrid
        .upsert_interest_owners(vec![
            (stale.clone(), vec![portable_log_interest()]),
            (retained.clone(), vec![portable_log_interest()]),
        ])
        .await
        .unwrap();
    for _ in 0..2 {
        let delivery = tokio::time::timeout(Duration::from_secs(2), hybrid.next_batch())
            .await
            .expect("post-coverage owner bootstrap timed out")
            .unwrap()
            .unwrap();
        hybrid
            .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
            .await
            .unwrap();
    }
    assert_eq!(hybrid.phase(), HybridPhase::Live);
    hybrid
        .replace_interest_owners_with_global_backfill(
            vec![
                (retained.clone(), vec![portable_log_interest()]),
                (added.clone(), vec![portable_log_interest()]),
            ],
            SubscriberBackfill::after_canonical_block(block_ref(101)).unwrap(),
        )
        .await
        .unwrap();

    assert!(hybrid.owner_interests(&stale).is_none());
    assert!(hybrid.owner_interests(&retained).is_some());
    assert!(hybrid.owner_interests(&added).is_some());
    assert_eq!(
        *registrations.lock().unwrap(),
        vec!["live", "history", "live", "history"]
    );
}

#[tokio::test]
async fn global_owner_replacement_commits_one_revision_per_source() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    let owner = HandlerId::new("restored-owner");
    hybrid
        .replace_interest_owners_with_global_backfill(
            vec![(owner.clone(), Vec::new())],
            SubscriberBackfill::after_canonical_block(block_ref(100)).unwrap(),
        )
        .await
        .unwrap();

    assert!(hybrid.owner_interests(&owner).is_some());
    assert_eq!(hybrid.phase(), HybridPhase::Live);
    assert_eq!(
        *registrations.lock().unwrap(),
        vec!["live", "history"],
        "live receives the exact topology and history receives the one global-backfill revision"
    );
}

#[tokio::test]
async fn historical_work_is_never_forwarded_to_the_live_child() {
    let global_calls = Arc::new(Mutex::new(Vec::new()));
    let history = LifecycleMethodProbeSubscriber {
        historical: true,
        calls: Arc::clone(&global_calls),
        steps: VecDeque::new(),
    };
    let live = LifecycleMethodProbeSubscriber {
        historical: false,
        calls: Arc::clone(&global_calls),
        steps: VecDeque::new(),
    };
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .replace_interest_owners_with_global_backfill(
            vec![(HandlerId::new("restored"), vec![portable_log_interest()])],
            SubscriberBackfill::after_canonical_block(block_ref(100)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        *global_calls.lock().unwrap(),
        vec!["live:replace", "history:replace-global-backfill"]
    );

    let add_calls = Arc::new(Mutex::new(Vec::new()));
    let history = LifecycleMethodProbeSubscriber {
        historical: true,
        calls: Arc::clone(&add_calls),
        steps: VecDeque::new(),
    };
    let live = LifecycleMethodProbeSubscriber {
        historical: false,
        calls: Arc::clone(&add_calls),
        steps: VecDeque::new(),
    };
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .add_interest_owner_with_backfill(
            HandlerId::new("dynamic"),
            &[portable_log_interest()],
            SubscriberBackfill::from_canonical_block(block_ref(100)),
        )
        .await
        .unwrap();
    assert_eq!(
        *add_calls.lock().unwrap(),
        vec!["live:add", "history:add-with-backfill"]
    );

    let canonical_calls = Arc::new(Mutex::new(Vec::new()));
    let history = LifecycleMethodProbeSubscriber {
        historical: true,
        calls: Arc::clone(&canonical_calls),
        steps: VecDeque::from([Step::Batch(
            Duration::from_millis(10),
            batch(&[99], b"canonical-probe-history"),
        )]),
    };
    let live = LifecycleMethodProbeSubscriber {
        historical: false,
        calls: Arc::clone(&canonical_calls),
        steps: VecDeque::from([Step::Batch(Duration::ZERO, tokenless_batch(&[100]))]),
    };
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .upsert_interest_owners(vec![(
            HandlerId::new("seed-owner"),
            vec![portable_log_interest()],
        )])
        .await
        .unwrap();
    for _ in 0..2 {
        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
            .await
            .unwrap();
    }
    canonical_calls.lock().unwrap().clear();
    hybrid
        .add_interest_owner_with_canonical_catchup(
            HandlerId::new("canonical-dynamic"),
            &[portable_log_interest()],
            block_ref(100),
        )
        .await
        .unwrap();
    assert_eq!(
        *canonical_calls.lock().unwrap(),
        vec!["live:add", "history:add-with-canonical-catchup"],
        "only history receives the retained baseline and historical activation work"
    );
}

#[tokio::test]
async fn duplicate_exact_owner_replacements_fail_before_mutating_either_child() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    let owner = HandlerId::new("duplicate");
    let duplicate = vec![(owner.clone(), Vec::new()), (owner.clone(), Vec::new())];

    let exact = hybrid
        .replace_interest_owners(duplicate.clone())
        .await
        .expect_err("duplicate exact topology");
    assert!(exact.to_string().contains("duplicate owner"));
    let global = hybrid
        .replace_interest_owners_with_global_backfill(
            duplicate,
            SubscriberBackfill::after_canonical_block(block_ref(100)).unwrap(),
        )
        .await
        .expect_err("duplicate global topology");
    assert!(global.to_string().contains("duplicate owner"));
    assert!(hybrid.owner_interests(&owner).is_none());
    assert!(registrations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn base_topology_rejects_owner_lifecycle_before_mutating_either_child() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("install pure base topology");
    let committed_calls = registrations.lock().unwrap().clone();
    let owner = HandlerId::new("owner-mode");

    let errors = [
        hybrid
            .replace_interest_owners(vec![(owner.clone(), Vec::new())])
            .await
            .expect_err("exact replacement cannot switch topology modes"),
        hybrid
            .replace_interest_owners_with_global_backfill(
                vec![(owner.clone(), Vec::new())],
                SubscriberBackfill::after_canonical_block(block_ref(100)).unwrap(),
            )
            .await
            .expect_err("global replacement cannot switch topology modes"),
        hybrid
            .upsert_interest_owners(vec![(owner.clone(), Vec::new())])
            .await
            .expect_err("bulk owner mutation cannot mix topology modes"),
        hybrid
            .add_interest_owner(owner.clone(), &[])
            .await
            .expect_err("owner mutation cannot mix topology modes"),
        hybrid
            .add_interest_owner_with_backfill(
                owner.clone(),
                &[],
                SubscriberBackfill::from_block(100),
            )
            .await
            .expect_err("owner backfill cannot mix topology modes"),
        hybrid
            .add_interest_owner_with_canonical_catchup(owner.clone(), &[], block_ref(100))
            .await
            .expect_err("canonical owner catch-up cannot mix topology modes"),
        hybrid
            .remove_interest_owner(&owner)
            .await
            .expect_err("owner removal cannot mix topology modes"),
    ];

    assert!(errors.iter().all(|error| {
        error.to_string().contains("base/unowned") && error.to_string().contains("owner-managed")
    }));
    assert_eq!(*registrations.lock().unwrap(), committed_calls);
}

#[tokio::test]
async fn owner_topology_rejects_base_replacement_before_mutating_either_child() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .upsert_interest_owners(vec![(HandlerId::new("owner-mode"), Vec::new())])
        .await
        .expect("install pure owner topology");
    let committed_calls = registrations.lock().unwrap().clone();

    let error = hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect_err("base replacement cannot switch topology modes");
    assert!(error.to_string().contains("base/unowned"));
    assert!(error.to_string().contains("owner-managed"));
    assert_eq!(*registrations.lock().unwrap(), committed_calls);
}

#[tokio::test]
async fn covered_base_can_clear_then_activate_owner_mode_only_with_global_backfill() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[100], b"base-clear-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::ZERO,
                batch(&[101], b"base-clear-live"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        hybrid.register_interests(&[]).await.unwrap();
        assert_eq!(hybrid.phase(), HybridPhase::Live);
        let cleared = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(cleared.delivery_token().unwrap().clone())
            .await
            .unwrap();

        hybrid
            .replace_interest_owners_with_global_backfill(
                vec![(
                    HandlerId::new("post-base-owner"),
                    vec![portable_log_interest()],
                )],
                SubscriberBackfill::after_canonical_block(block_ref(101)).unwrap(),
            )
            .await
            .expect("base-to-owner global backfill activation");
        assert_eq!(hybrid.phase(), HybridPhase::CatchingUp);
    })
    .await
    .expect("base-to-owner transition regression timed out");
}

#[tokio::test]
async fn covered_owner_can_clear_but_cannot_reactivate_base_without_fresh_restore() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[100], b"owner-clear-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::ZERO,
                batch(&[101], b"owner-clear-live"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .upsert_interest_owners(vec![(
                HandlerId::new("owner-to-clear"),
                vec![portable_log_interest()],
            )])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        hybrid.replace_interest_owners(Vec::new()).await.unwrap();
        assert_eq!(hybrid.phase(), HybridPhase::Live);
        let cleared = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(cleared.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let calls_after_clear = registrations.lock().unwrap().clone();

        let error = hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .expect_err("owner-to-base requires fresh authoritative restore");
        assert!(
            error
                .to_string()
                .contains("no atomic global-backfill rollback primitive")
        );
        assert_eq!(*registrations.lock().unwrap(), calls_after_clear);
    })
    .await
    .expect("owner-to-base transition regression timed out");
}

#[tokio::test]
async fn restore_preparation_rejects_mixed_topology_before_mutating_live() {
    let (token, checkpoint) = synthetic_resume_fixture().await;
    let position = resume_position(105, vec![block_ref(104), block_ref(105)], token, checkpoint);
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();

    let error = hybrid
        .prepare_restore_lifecycle(
            &position,
            &[portable_log_interest()],
            vec![(HandlerId::new("mixed-owner"), Vec::new())],
        )
        .await
        .expect_err("mixed restored topology must fail closed");
    assert!(error.to_string().contains("base/unowned"));
    assert!(error.to_string().contains("owner-managed"));
    assert!(registrations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn owner_restore_preparation_exactly_replaces_only_the_ephemeral_live_topology() {
    let owner = HandlerId::new("restored-owner");
    let interests = vec![portable_log_interest()];
    let initial_calls = Arc::new(Mutex::new(Vec::new()));
    let history = LifecycleMethodProbeSubscriber {
        historical: true,
        calls: Arc::clone(&initial_calls),
        steps: VecDeque::from([Step::Batch(
            Duration::from_millis(10),
            batch(&[104], b"owner-restore-history"),
        )]),
    };
    let live = LifecycleMethodProbeSubscriber {
        historical: false,
        calls: initial_calls,
        steps: VecDeque::from([Step::Batch(Duration::ZERO, tokenless_batch(&[105]))]),
    };
    let mut first = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    first
        .upsert_interest_owners(vec![(owner.clone(), interests.clone())])
        .await
        .unwrap();
    let historical = first.next_batch().await.unwrap().unwrap();
    first
        .acknowledge_delivery(historical.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = first.next_batch().await.unwrap().unwrap();
    let token = live.delivery_token().unwrap().clone();
    let checkpoint = live.subscriber_checkpoint().unwrap().clone();
    first.acknowledge_delivery(token.clone()).await.unwrap();
    let position = resume_position(105, vec![block_ref(104), block_ref(105)], token, checkpoint);

    let restore_calls = Arc::new(Mutex::new(Vec::new()));
    let history = LifecycleMethodProbeSubscriber {
        historical: true,
        calls: Arc::clone(&restore_calls),
        steps: VecDeque::new(),
    };
    let live = LifecycleMethodProbeSubscriber {
        historical: false,
        calls: Arc::clone(&restore_calls),
        steps: VecDeque::new(),
    };
    let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    restored
        .prepare_restore_lifecycle(&position, &[], vec![(owner.clone(), interests.clone())])
        .await
        .unwrap();
    assert_eq!(
        *restore_calls.lock().unwrap(),
        vec!["live:replace"],
        "ephemeral restore must exact-replace live owners without mutating durable history"
    );
    restored.restore_position(&position).unwrap();
    assert_eq!(
        restored.owner_interests(&owner).map(<[_]>::len),
        Some(interests.len())
    );
}

#[tokio::test]
async fn restored_owner_can_be_removed_readded_checkpointed_and_acknowledged() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let owner = HandlerId::new("restored-readd-owner");
        let interests = vec![portable_log_interest()];
        let source_calls = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                scoped_batch(&[100], owner.clone(), b"restored-readd-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&source_calls),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::ZERO,
                batch(&[101], b"restored-readd-live"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            source_calls,
        );
        let mut source = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        source
            .upsert_interest_owners(vec![(owner.clone(), interests.clone())])
            .await
            .unwrap();
        let historical = source.next_batch().await.unwrap().unwrap();
        source
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let live = source.next_batch().await.unwrap().unwrap();
        let position = resume_position(
            101,
            vec![block_ref(100), block_ref(101)],
            live.delivery_token().unwrap().clone(),
            live.subscriber_checkpoint().unwrap().clone(),
        );
        source
            .acknowledge_delivery(live.delivery_token().unwrap().clone())
            .await
            .unwrap();

        let restore_calls = Arc::new(Mutex::new(Vec::new()));
        let readd_catchup = owner_catchup_batch(
            [record(102)],
            owner.clone(),
            b"restored-readd-owner-catchup",
        )
        .with_chain_controls([ChainControl::CanonicalProgress(block_ref(102))]);
        let history = ScriptedSubscriber::new(
            "history",
            [
                Step::Batch(
                    Duration::from_millis(10),
                    batch(&[101], b"restored-readd-recovery"),
                ),
                Step::Batch(Duration::from_millis(10), readd_catchup),
            ],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&restore_calls),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[102], b"restored-readd-live-102")),
                Step::Batch(Duration::ZERO, batch(&[103], b"restored-readd-live-103")),
            ],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&restore_calls),
        );
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        restored
            .prepare_restore_lifecycle(&position, &[], vec![(owner.clone(), interests.clone())])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();

        let recovery = restored.next_batch().await.unwrap().unwrap();
        restored
            .acknowledge_delivery(recovery.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let recovered_live = restored.next_batch().await.unwrap().unwrap();
        restored
            .acknowledge_delivery(recovered_live.delivery_token().unwrap().clone())
            .await
            .unwrap();
        assert_eq!(restored.phase(), HybridPhase::Live);

        assert!(
            restored
                .remove_interest_owner(&owner)
                .await
                .unwrap()
                .is_some()
        );
        let removed_barrier = restored.next_batch().await.unwrap().unwrap();
        restored
            .acknowledge_delivery(removed_barrier.delivery_token().unwrap().clone())
            .await
            .unwrap();
        restored
            .add_interest_owner_with_backfill(
                owner.clone(),
                &interests,
                SubscriberBackfill::from_block(102),
            )
            .await
            .unwrap();
        let replay = restored.next_batch().await.unwrap().unwrap();
        assert!(replay.subscriber_checkpoint().is_some());
        assert_eq!(
            replay.record_audience(0),
            Some(&DeliveryAudience::Owners(vec![owner]))
        );
        restored
            .acknowledge_delivery(replay.delivery_token().unwrap().clone())
            .await
            .unwrap();
        assert!(restored.poison_reason().is_none());
    })
    .await
    .expect("restored owner remove/re-add regression timed out");
}

#[tokio::test]
async fn canonical_owner_catchup_rejects_an_unretained_anchor_before_mutating_children() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let history = LifecycleMethodProbeSubscriber {
        historical: true,
        calls: Arc::clone(&calls),
        steps: VecDeque::new(),
    };
    let live = LifecycleMethodProbeSubscriber {
        historical: false,
        calls: Arc::clone(&calls),
        steps: VecDeque::new(),
    };
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();

    let error = hybrid
        .add_interest_owner_with_canonical_catchup(
            HandlerId::new("unretained"),
            &[portable_log_interest()],
            block_ref(100),
        )
        .await
        .expect_err("canonical catch-up requires exact retained Hybrid history");
    assert!(
        error
            .to_string()
            .contains("outside retained hybrid history")
    );
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failed_initial_exact_replacement_restores_empty_topology_on_both_children() {
    let owner_b = HandlerId::new("owner-b");
    let historical_owners = Arc::new(Mutex::new(Vec::new()));
    let live_owners = Arc::new(Mutex::new(Vec::new()));
    let history = ExactTopologySubscriber {
        historical: true,
        steps: VecDeque::from([Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"topology-history"),
        )]),
        owners: Arc::clone(&historical_owners),
        // The replacement mutates before reporting failure, then reconciliation
        // must exact-restore the initial empty topology before another operation
        // may proceed.
        replace_results: VecDeque::from([Err("historical commit uncertain"), Ok(())]),
    };
    let live = ExactTopologySubscriber {
        historical: false,
        steps: VecDeque::from([Step::Batch(Duration::ZERO, tokenless_batch(&[101]))]),
        owners: Arc::clone(&live_owners),
        replace_results: VecDeque::from([Ok(()), Ok(())]),
    };
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();

    let error = hybrid
        .replace_interest_owners(vec![(owner_b.clone(), Vec::new())])
        .await
        .expect_err("historical side reports an uncertain replacement");
    assert!(
        error.to_string().contains("historical commit uncertain"),
        "unexpected replacement error: {error}"
    );
    assert!(hybrid.owner_interests(&owner_b).is_none());
    assert!(historical_owners.lock().unwrap().is_empty());
    assert!(live_owners.lock().unwrap().is_empty());

    hybrid
        .replace_interest_owners(vec![(owner_b.clone(), Vec::new())])
        .await
        .expect("a reconciled coordinator accepts a clean retry");
    assert!(hybrid.owner_interests(&owner_b).is_some());
    assert_eq!(*historical_owners.lock().unwrap(), vec![owner_b.clone()]);
    assert_eq!(*live_owners.lock().unwrap(), vec![owner_b]);
}

#[tokio::test]
async fn historical_replay_is_not_deduplicated_before_ack() {
    let history_acks = Arc::new(Mutex::new(Vec::new()));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [
            Step::Batch(Duration::from_millis(10), batch(&[20], b"same")),
            Step::Batch(Duration::from_millis(10), batch(&[20], b"same")),
        ],
        Arc::clone(&history_acks),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[21], b"live")),
            Step::End(Duration::from_millis(100)),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let first = hybrid.next_batch().await.unwrap().unwrap();
    let replay = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(first.records().len(), 1);
    assert_eq!(
        replay.records().len(),
        1,
        "unacknowledged replay must remain intact"
    );
    hybrid
        .acknowledge_delivery(replay.delivery_token().unwrap().clone())
        .await
        .expect("ack replay");
    assert_eq!(*history_acks.lock().unwrap(), vec![b"same".to_vec()]);
}

#[tokio::test]
async fn payload_commitment_is_preserved_from_both_hybrid_children() {
    let history_commitment = SubscriberPayloadCommitment::new(B256::repeat_byte(0x71));
    let live_commitment = SubscriberPayloadCommitment::new(B256::repeat_byte(0x72));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"committed-history").with_payload_commitment(history_commitment),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(
            Duration::ZERO,
            tokenless_batch(&[101]).with_payload_commitment(live_commitment),
        )],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let history = tokio::time::timeout(Duration::from_secs(2), hybrid.next_batch())
        .await
        .expect("payload-commitment history delivery timed out")
        .unwrap()
        .unwrap();
    assert_eq!(history.payload_commitment(), Some(&history_commitment));
    hybrid
        .acknowledge_delivery(history.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = tokio::time::timeout(Duration::from_secs(2), hybrid.next_batch())
        .await
        .expect("payload-commitment live delivery timed out")
        .unwrap()
        .unwrap();
    assert_eq!(live.payload_commitment(), Some(&live_commitment));
}

#[tokio::test]
async fn committed_overlap_for_one_owner_does_not_starve_a_new_owner() {
    let owner_a = HandlerId::new("owner-a");
    let owner_b = HandlerId::new("owner-b");
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [
            Step::Batch(
                Duration::from_millis(10),
                scoped_batch(&[100], owner_a.clone(), b"history-a"),
            ),
            Step::Batch(
                Duration::from_millis(20),
                scoped_batch(&[100], owner_b.clone(), b"history-b")
                    .with_chain_controls([ChainControl::CanonicalProgress(block_ref(102))]),
            ),
            Step::Batch(
                Duration::from_millis(20),
                scoped_batch(&[100], owner_b.clone(), b"history-b-again"),
            ),
        ],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
            Step::Batch(Duration::ZERO, batch(&[102], b"live-102")),
            Step::Batch(Duration::ZERO, batch(&[103], b"live-103")),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .upsert_interest_owners(vec![(owner_a.clone(), vec![portable_log_interest()])])
        .await
        .expect("register owner A");

    let first = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(
        first.record_audience(0),
        Some(&DeliveryAudience::Owners(vec![owner_a]))
    );
    hybrid
        .acknowledge_delivery(first.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(live.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(live.delivery_token().unwrap().clone())
        .await
        .unwrap();

    hybrid
        .add_interest_owner_with_backfill(
            owner_b.clone(),
            &[portable_log_interest()],
            SubscriberBackfill::from_block(100),
        )
        .await
        .expect("add owner B");
    let replay_for_b = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(replay_for_b.records().len(), 1);
    assert_eq!(
        replay_for_b.record_audience(0),
        Some(&DeliveryAudience::Owners(vec![owner_b.clone()]))
    );
    hybrid
        .acknowledge_delivery(replay_for_b.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let buffered_live = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(buffered_live.delivery_token().unwrap().clone())
        .await
        .unwrap();
    hybrid.remove_interest_owner(&owner_b).await.unwrap();
    hybrid
        .add_interest_owner_with_backfill(
            owner_b.clone(),
            &[portable_log_interest()],
            SubscriberBackfill::from_block(100),
        )
        .await
        .unwrap();
    let replay_after_readd = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(replay_after_readd.records().len(), 1);
    assert_eq!(
        replay_after_readd.record_audience(0),
        Some(&DeliveryAudience::Owners(vec![owner_b]))
    );
}

#[tokio::test]
async fn same_input_for_distinct_owners_is_merged_without_losing_audience() {
    let owner_a = HandlerId::new("owner-a");
    let owner_b = HandlerId::new("owner-b");
    let duplicated = ReactiveInputBatch::from_scoped_records([
        (record(100), DeliveryAudience::Owners(vec![owner_a.clone()])),
        (record(100), DeliveryAudience::Owners(vec![owner_b.clone()])),
    ])
    .with_delivery_token(SubscriberDeliveryToken::new(b"history".to_vec()));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::from_millis(10), duplicated)],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, batch(&[101], b"live"))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .upsert_interest_owners(vec![
            (owner_a.clone(), vec![portable_log_interest()]),
            (owner_b.clone(), vec![portable_log_interest()]),
        ])
        .await
        .expect("register owners");

    let delivered = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(delivered.records().len(), 1);
    assert_eq!(
        delivered.record_audience(0),
        Some(&DeliveryAudience::Owners(vec![owner_a, owner_b]))
    );
}

#[tokio::test]
async fn malformed_owner_routing_is_rejected_before_buffering() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let owner = HandlerId::new("owner");
        let cases = [
            (
                DeliveryAudience::Owners(Vec::new()),
                DeliveryScope::Canonical,
                "empty owner audience",
            ),
            (
                DeliveryAudience::Owners(vec![owner.clone(), owner.clone()]),
                DeliveryScope::Canonical,
                "duplicate owner",
            ),
            (
                DeliveryAudience::AllExcept(vec![owner.clone(), owner.clone()]),
                DeliveryScope::Canonical,
                "duplicate owner",
            ),
            (
                DeliveryAudience::All,
                DeliveryScope::OwnerCatchup,
                "non-empty exact owner audience",
            ),
        ];

        for (audience, scope, expected) in cases {
            let registrations = Arc::new(Mutex::new(Vec::new()));
            let history = ScriptedSubscriber::new(
                "history",
                [Step::Batch(
                    Duration::from_millis(50),
                    batch(&[100], b"history"),
                )],
                Arc::new(Mutex::new(Vec::new())),
                Arc::clone(&registrations),
            );
            let malformed = ReactiveInputBatch::from_deliveries([ReactiveInputDelivery::new(
                record(101),
                audience,
                scope,
            )])
            .with_delivery_token(SubscriberDeliveryToken::new(b"malformed".to_vec()));
            let live = ScriptedSubscriber::new(
                "live",
                [Step::Batch(Duration::ZERO, malformed)],
                Arc::new(Mutex::new(Vec::new())),
                registrations,
            );
            let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
            hybrid
                .register_interests(&[portable_log_interest()])
                .await
                .unwrap();

            let error = hybrid.next_batch().await.expect_err("malformed routing");
            assert!(
                error.to_string().contains(expected),
                "unexpected routing error: {error}"
            );
            assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
            assert_eq!(hybrid.buffered_live_batches(), 0);
        }
    })
    .await
    .expect("malformed-routing regression timed out");
}

#[tokio::test]
async fn malformed_chain_controls_are_rejected_before_buffering() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let cases = [
            (
                ChainControl::Barrier {
                    id: Vec::new(),
                    block: Some(block_ref(101)),
                },
                "empty barrier id",
            ),
            (
                ChainControl::Reorg {
                    common_ancestor: block_ref(102),
                    old_tip: block_ref(101),
                    new_tip: block_ref(103),
                },
                "invalid reorg ancestry triple",
            ),
        ];

        for (control, expected) in cases {
            let registrations = Arc::new(Mutex::new(Vec::new()));
            let history = ScriptedSubscriber::new(
                "history",
                [Step::Batch(
                    Duration::from_millis(50),
                    batch(&[100], b"history"),
                )],
                Arc::new(Mutex::new(Vec::new())),
                Arc::clone(&registrations),
            );
            let malformed = ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([control])
                .with_delivery_token(SubscriberDeliveryToken::new(b"malformed".to_vec()));
            let live = ScriptedSubscriber::new(
                "live",
                [Step::Batch(Duration::ZERO, malformed)],
                Arc::new(Mutex::new(Vec::new())),
                registrations,
            );
            let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
            hybrid
                .register_interests(&[portable_log_interest()])
                .await
                .unwrap();

            let error = hybrid.next_batch().await.expect_err("malformed control");
            assert!(
                error.to_string().contains(expected),
                "unexpected control error: {error}"
            );
            assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
            assert_eq!(hybrid.buffered_live_batches(), 0);
        }
    })
    .await
    .expect("malformed-control regression timed out");
}

#[tokio::test]
async fn finality_state_rejects_a_cross_source_safe_head_regression() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history_batch = batch(&[100], b"history-safe-100")
            .with_chain_controls([ChainControl::Safe(block_ref(100))]);
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::from_millis(10), history_batch)],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let regressed_safe = ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Safe(block_ref(99))])
            .with_delivery_token(SubscriberDeliveryToken::new(b"safe-99".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                Step::Batch(Duration::from_millis(10), regressed_safe),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }

        let error = hybrid.next_batch().await.expect_err("safe head regressed");
        assert!(
            error.to_string().contains("safe head")
                && (error.to_string().contains("conflict")
                    || error.to_string().contains("regress")),
            "unexpected safe regression error: {error}"
        );
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("safe-head regression timed out");
}

#[tokio::test]
async fn finalized_head_rejects_a_cross_source_rewind_below_finality() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history_batch = batch(&[99, 100], b"history-finalized-100")
            .with_chain_controls([ChainControl::Finalized(block_ref(100))]);
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::from_millis(10), history_batch)],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let mut replacement_tip = block_ref(101);
        replacement_tip.hash = B256::repeat_byte(0xe1);
        // The common ancestor is sparse/non-adjacent, so the replacement tip
        // cannot truthfully name it as its direct parent. Leave that metadata
        // absent to isolate the intended rollback-below-finality failure.
        replacement_tip.parent_hash = None;
        let invalid_reorg = reorg_batch(
            block_ref(99),
            block_ref(101),
            replacement_tip,
            replacement_record(101, 0xe1, 0xe2),
            b"rewind-finalized",
        );
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                Step::Batch(Duration::from_millis(10), invalid_reorg),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }

        let error = hybrid
            .next_batch()
            .await
            .expect_err("rewind below finalized head");
        assert!(
            error.to_string().contains("finalized"),
            "unexpected finalized-rewind error: {error}"
        );
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("finalized-rewind regression timed out");
}

#[tokio::test]
async fn canonical_batch_rejects_conflicting_identities_at_one_height() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let mut left_block = block_ref(100);
        left_block.hash = B256::repeat_byte(0xa1);
        let mut right_block = block_ref(100);
        right_block.hash = B256::repeat_byte(0xa2);
        let conflicting = ReactiveInputBatch::new(vec![
            record_for_block(left_block, B256::repeat_byte(0xb1)),
            record_for_block(right_block, B256::repeat_byte(0xb2)),
        ])
        .with_delivery_token(SubscriberDeliveryToken::new(b"conflicting-height".to_vec()));
        let history = ScriptedSubscriber::new(
            "history",
            [
                Step::Batch(Duration::ZERO, batch(&[99], b"conflict-seed")),
                Step::Batch(Duration::ZERO, conflicting),
            ],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live =
            ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), registrations);
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let seed = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(seed.delivery_token().unwrap().clone())
            .await
            .unwrap();

        let error = hybrid
            .next_batch()
            .await
            .expect_err("conflicting canonical identities");
        assert!(
            error.to_string().contains("conflicting")
                || error.to_string().contains("conflicts with block identity")
                || error.to_string().contains("same height")
                || error.to_string().contains("canonical replacement"),
            "unexpected same-height conflict error: {error}"
        );
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("same-height conflict regression timed out");
}

#[tokio::test]
async fn live_failure_uses_history_to_fill_gap_then_cuts_back() {
    let history_acks = Arc::new(Mutex::new(Vec::new()));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [
            Step::Batch(Duration::from_millis(10), batch(&[100], b"h100")),
            // 102 overlaps a live batch already acknowledged before failure;
            // 103 is the actual gap filler and recovered-live fence.
            Step::Batch(Duration::from_millis(20), batch(&[101, 102, 103], b"h103")),
        ],
        Arc::clone(&history_acks),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[101], b"l101")),
            Step::Batch(Duration::from_millis(50), batch(&[102], b"l102")),
            Step::Error(Duration::ZERO, "websocket exhausted reconnects"),
            Step::Batch(Duration::ZERO, batch(&[104], b"l104")),
            Step::End(Duration::from_millis(100)),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.recent_input_capacity = 4;
    let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let initial_history = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(initial_history.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let buffered_101 = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(buffered_101.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live_102 = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(live_102.delivery_token().unwrap().clone())
        .await
        .unwrap();

    let failure = hybrid
        .next_batch()
        .await
        .expect_err("live failure surfaces");
    assert!(
        failure
            .to_string()
            .contains("websocket exhausted reconnects")
    );
    assert_eq!(hybrid.phase(), HybridPhase::Recovering);

    let recovered_history = hybrid.next_batch().await.unwrap().unwrap();
    let recovered_blocks: Vec<_> = recovered_history
        .records()
        .iter()
        .map(|record| record.context.block.as_ref().unwrap().number)
        .collect();
    assert_eq!(
        recovered_blocks,
        vec![103],
        "live/history overlap must be suppressed"
    );
    assert_eq!(hybrid.fence(), Some(103));
    hybrid
        .acknowledge_delivery(recovered_history.delivery_token().unwrap().clone())
        .await
        .unwrap();
    assert_eq!(hybrid.phase(), HybridPhase::DrainingLive);

    let recovered_live = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(
        recovered_live.records()[0]
            .context
            .block
            .as_ref()
            .unwrap()
            .number,
        104
    );
}

#[tokio::test]
async fn dedupe_journal_rewinds_when_a_branch_is_removed_then_reincluded() {
    let ancestor = block_ref(100);
    let branch_a = block_ref(101);
    let branch_b = BlockRef {
        number: 101,
        hash: B256::repeat_byte(0xee),
        parent_hash: Some(ancestor.hash),
        timestamp: branch_a.timestamp,
    };
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[101], b"branch-a")),
            Step::Batch(
                Duration::from_millis(30),
                reorg_batch(
                    ancestor,
                    branch_a,
                    branch_b,
                    record_for_block(branch_b, B256::repeat_byte(0xef)),
                    b"branch-b",
                ),
            ),
            Step::Batch(
                Duration::ZERO,
                reorg_batch(
                    ancestor,
                    branch_b,
                    branch_a,
                    record_for_block(branch_a, B256::repeat_byte(102)),
                    b"branch-a-again",
                ),
            ),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();
    let history = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(history.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let first_a = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(first_a.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let branch_b = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(branch_b.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let reinserted_a = hybrid.next_batch().await.unwrap().unwrap();

    assert_eq!(
        reinserted_a.records().len(),
        1,
        "an input rolled back with branch A must be deliverable when A becomes canonical again"
    );
}

#[tokio::test]
async fn explicit_reorg_accepts_a_sparse_retained_common_ancestor() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let ancestor = block_ref(103);
        let old_tip = block_ref(105);
        let mut new_tip = block_ref(106);
        new_tip.hash = B256::repeat_byte(0xee);
        new_tip.parent_hash = None;

        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100, 104], b"sparse-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[105], b"old-tip")),
                Step::Batch(
                    Duration::from_millis(10),
                    reorg_batch(
                        ancestor,
                        old_tip,
                        new_tip,
                        record_for_block(new_tip, B256::repeat_byte(0xef)),
                        b"sparse-reorg",
                    ),
                ),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid =
            HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();

        let historical = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let old_branch = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(old_branch.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let replacement = hybrid
            .next_batch()
            .await
            .expect("sparse reorg is valid")
            .expect("replacement delivery");

        assert!(matches!(
            replacement.chain_controls(),
            [ChainControl::Reorg { common_ancestor, old_tip: observed_old_tip, new_tip: observed_new_tip }]
                if *common_ancestor == ancestor
                    && *observed_old_tip == old_tip
                    && *observed_new_tip == new_tip
        ));
        assert_eq!(replacement.records().len(), 1);
    })
    .await
    .expect("sparse explicit-reorg regression timed out");
}

#[tokio::test]
async fn historical_recovery_accepts_a_sparse_retained_common_ancestor() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let ancestor = block_ref(103);
        let mut new_tip = block_ref(106);
        new_tip.hash = B256::repeat_byte(0xec);
        new_tip.parent_hash = None;
        let recovery = reorg_batch(
            ancestor,
            block_ref(105),
            new_tip,
            record_for_block(new_tip, B256::repeat_byte(0xed)),
            b"sparse-recovery-reorg",
        );
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [
                Step::Batch(
                    Duration::from_millis(10),
                    batch(&[100, 104], b"sparse-history"),
                ),
                Step::Batch(Duration::from_millis(10), recovery),
            ],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[105], b"live-105")),
                Step::Error(Duration::from_millis(10), "live failed"),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        hybrid.next_batch().await.expect_err("live failure");
        assert_eq!(hybrid.phase(), HybridPhase::Recovering);

        let replacement = hybrid.next_batch().await.unwrap().unwrap();
        assert!(matches!(
            replacement.chain_controls(),
            [ChainControl::Reorg { common_ancestor, old_tip, new_tip: observed }]
                if *common_ancestor == ancestor
                    && *old_tip == block_ref(105)
                    && *observed == new_tip
        ));
    })
    .await
    .expect("sparse historical-recovery regression timed out");
}

#[tokio::test]
async fn removed_log_is_delivered_again_after_its_branch_is_reincluded() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let branch = block_ref(101);
        let transaction = B256::repeat_byte(102);
        let removed = || {
            ReactiveInputBatch::new(vec![removed_record_for_block(branch, transaction)])
                .with_delivery_token(SubscriberDeliveryToken::new(Vec::new()))
        };
        let mut first_removed = removed();
        first_removed = first_removed
            .with_delivery_token(SubscriberDeliveryToken::new(b"removed-once".to_vec()));
        let mut second_removed = removed();
        second_removed = second_removed
            .with_delivery_token(SubscriberDeliveryToken::new(b"removed-twice".to_vec()));

        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"history-100"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"branch-first")),
                Step::Batch(Duration::from_millis(10), first_removed),
                Step::Batch(Duration::ZERO, batch(&[101], b"branch-reincluded")),
                Step::Batch(Duration::ZERO, second_removed),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid =
            HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();

        for _ in 0..4 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }

        let removed_again = hybrid.next_batch().await.unwrap().unwrap();
        assert_eq!(
            removed_again.records().len(),
            1,
            "a new removal lifecycle must not collapse into a token-only checkpoint rewind"
        );
        assert!(matches!(
            removed_again.records()[0].context.chain_status,
            ChainStatus::Reorged { .. }
        ));
    })
    .await
    .expect("repeated removed-log regression timed out");
}

#[tokio::test]
async fn reorg_after_post_record_progress_is_rejected_before_delivery_or_ack() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[100], b"history-100"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let replacement_tip = BlockRef {
            number: 103,
            hash: B256::repeat_byte(0xee),
            parent_hash: Some(block_ref(102).hash),
            timestamp: block_ref(103).timestamp,
        };
        let ordered_controls = ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([
                ChainControl::CanonicalProgress(block_ref(102)),
                ChainControl::Reorg {
                    common_ancestor: block_ref(101),
                    old_tip: block_ref(102),
                    new_tip: replacement_tip,
                },
            ])
            .with_delivery_token(SubscriberDeliveryToken::new(b"progress-reorg".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                Step::Batch(Duration::from_millis(30), ordered_controls),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        let error = hybrid
            .next_batch()
            .await
            .expect_err("post-record progress cannot precede a reorg in one envelope");
        assert!(error.to_string().contains("reorg controls must precede"));
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("ordered-control regression timed out");
}

#[tokio::test]
async fn historical_reorg_is_rebased_to_the_current_live_tip() {
    let branch_b = BlockRef {
        number: 103,
        hash: B256::repeat_byte(0xee),
        parent_hash: Some(block_ref(102).hash),
        timestamp: block_ref(103).timestamp,
    };
    let reorg = reorg_batch(
        block_ref(102),
        block_ref(103),
        branch_b,
        record_for_block(branch_b, B256::repeat_byte(0xef)),
        b"historical-reorg",
    );
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [
            Step::Batch(
                Duration::from_millis(10),
                batch(&[100, 101, 102, 103], b"history-103"),
            ),
            Step::Batch(Duration::from_millis(20), reorg),
        ],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[104], b"live-104")),
            Step::Batch(Duration::from_millis(30), batch(&[105], b"live-105")),
            Step::Error(Duration::ZERO, "live failed"),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();
    for _ in 0..3 {
        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
            .await
            .unwrap();
    }
    hybrid.next_batch().await.expect_err("live failure");
    let replacement = hybrid.next_batch().await.unwrap().unwrap();
    let ChainControl::Reorg { old_tip, .. } = &replacement.chain_controls()[0] else {
        panic!("expected rebased reorg control");
    };
    assert_eq!(old_tip.number, 105);
    assert_eq!(old_tip.hash, block_ref(105).hash);
}

#[tokio::test]
async fn tokenless_live_delivery_is_ack_gated_without_forwarding_a_synthetic_ack() {
    let live_acks = Arc::new(Mutex::new(Vec::new()));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(20),
            batch(&[104], b"h104"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, tokenless_batch(&[105])),
            Step::Batch(Duration::from_millis(10), tokenless_batch(&[105])),
        ],
        Arc::clone(&live_acks),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");
    let history = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(history.delivery_token().unwrap().clone())
        .await
        .unwrap();

    let first = hybrid.next_batch().await.unwrap().unwrap();
    let first_token = first
        .delivery_token()
        .expect("coordinator synthesizes a token")
        .clone();
    let replay_before_ack = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(
        replay_before_ack.records()[0].input_ref(),
        first.records()[0].input_ref(),
        "deduplication must not commit tokenless input identity before runtime ingestion"
    );
    hybrid
        .acknowledge_delivery(first_token)
        .await
        .expect("commit synthetic coordinator token");
    assert!(
        live_acks.lock().expect("live acks").is_empty(),
        "a coordinator-generated token must not be forwarded to the live source"
    );
}

#[tokio::test]
async fn restored_coordinator_never_reuses_the_last_synthetic_token() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[104], b"history-104"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[105]))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut first =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    first
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");
    let history = first.next_batch().await.unwrap().unwrap();
    first
        .acknowledge_delivery(history.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = first.next_batch().await.unwrap().unwrap();
    let restored_token = live.delivery_token().unwrap().clone();
    let restored_checkpoint = live.subscriber_checkpoint().unwrap().clone();
    first
        .acknowledge_delivery(restored_token.clone())
        .await
        .unwrap();

    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[105], b"history-105"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[106]))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut restored =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    let position = SubscriberResumePosition::new(
        1,
        block_ref(105),
        vec![block_ref(104), block_ref(105)],
        Some(restored_token.clone()),
        Some(restored_checkpoint),
    );
    restored
        .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
        .await
        .expect("prepare restored lifecycle");
    restored
        .restore_position(&position)
        .expect("restore coordinator");

    let catchup = restored.next_batch().await.unwrap().unwrap();
    assert!(
        catchup.records().is_empty(),
        "the durable coordinator journal must suppress already committed overlap after restart"
    );
    restored
        .acknowledge_delivery(catchup.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let new_live = restored.next_batch().await.unwrap().unwrap();
    assert_eq!(
        new_live.records()[0].context.block.as_ref().unwrap().number,
        106
    );
    assert_ne!(new_live.delivery_token(), Some(&restored_token));
}

#[tokio::test]
async fn restore_preparation_rejects_runtime_history_that_conflicts_with_the_hybrid_checkpoint() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[104], b"history-104"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[105]))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut first =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    first
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");
    let historical = first.next_batch().await.unwrap().unwrap();
    first
        .acknowledge_delivery(historical.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = first.next_batch().await.unwrap().unwrap();
    let token = live.delivery_token().unwrap().clone();
    let checkpoint = live.subscriber_checkpoint().unwrap().clone();
    first.acknowledge_delivery(token.clone()).await.unwrap();

    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), registrations);
    let mut restored =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    let mut conflicting_104 = block_ref(104);
    conflicting_104.hash = B256::repeat_byte(0xee);
    let mut conflicting_105 = block_ref(105);
    conflicting_105.parent_hash = Some(conflicting_104.hash);
    let position = SubscriberResumePosition::new(
        1,
        block_ref(105),
        vec![conflicting_104, conflicting_105],
        Some(token),
        Some(checkpoint),
    );
    let error = restored
        .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
        .await
        .expect_err("conflicting retained history must fail before child mutation");
    let message = error.to_string();
    assert!(
        message.contains("parent-hash discontinuity")
            || message.contains("runtime canonical history conflicts")
            || message.contains("does not end at its coverage head"),
        "unexpected restore error: {message}"
    );
}

#[tokio::test]
async fn hybrid_preserves_record_audience_controls_and_checkpoint() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let owner = HandlerId::new("scoped-owner");
    let live_batch = ReactiveInputBatch::from_scoped_records([(
        record(105),
        DeliveryAudience::Owners(vec![owner.clone()]),
    )])
    .with_chain_controls([ChainControl::Barrier {
        id: b"live-fence".to_vec(),
        block: None,
    }])
    .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"live-checkpoint".to_vec()))
    .with_delivery_token(SubscriberDeliveryToken::new(b"l105".to_vec()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(20),
            batch(&[104], b"h104"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, live_batch)],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .upsert_interest_owners(vec![(owner.clone(), vec![portable_log_interest()])])
        .await
        .expect("register owner");
    let historical = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(historical.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let delivered = hybrid.next_batch().await.unwrap().unwrap();

    assert_eq!(
        delivered.record_audience(0),
        Some(&DeliveryAudience::Owners(vec![owner]))
    );
    assert_eq!(
        delivered.chain_controls(),
        &[ChainControl::Barrier {
            id: b"live-fence".to_vec(),
            block: None,
        }]
    );
    assert_ne!(
        delivered
            .subscriber_checkpoint()
            .expect("checkpoint")
            .as_bytes(),
        b"live-checkpoint",
        "Hybrid must wrap the source cursor in its durable coordinator checkpoint"
    );
}

#[tokio::test]
async fn buffered_progress_already_proven_by_history_is_not_reapplied() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let live_batch = batch(&[105], b"live").with_chain_controls([
        ChainControl::CanonicalProgress(block_ref(104)),
        ChainControl::Barrier {
            id: b"duplicate".to_vec(),
            block: Some(block_ref(104)),
        },
    ]);
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[104], b"history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, live_batch)],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();
    let history = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(history.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = hybrid.next_batch().await.unwrap().unwrap();
    assert!(live.chain_controls().is_empty());
    assert_eq!(
        live.records()[0].context.block.as_ref().unwrap().number,
        105
    );
}

#[tokio::test]
async fn failed_lifecycle_compensation_blocks_delivery_until_retry_reconciles_both_sources() {
    let history = FailingRegistrationSubscriber {
        results: VecDeque::from([Err("historical registration failed")]),
    };
    let live = FailingRegistrationSubscriber {
        results: VecDeque::from([Ok(()), Err("live rollback failed")]),
    };
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    let error = hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect_err("partial lifecycle failure");
    assert!(error.to_string().contains("rollback remains pending"));
    assert_ne!(hybrid.phase(), HybridPhase::Poisoned);
    assert!(hybrid.poison_reason().is_none());
    assert!(
        hybrid
            .next_batch()
            .await
            .expect_err("unreconciled coordinator fails closed")
            .to_string()
            .contains("lifecycle reconciliation is pending")
    );
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("retry reconciles both sources before committing");
}

#[tokio::test]
async fn post_coverage_base_clear_failure_poisons_instead_of_hiding_the_mutation_gap() {
    let (mut hybrid, signals) = covered_gap_mutation_hybrid(None).await;
    signals.fail_historical.store(true, Ordering::SeqCst);

    let error = hybrid
        .register_interests(&[])
        .await
        .expect_err("historical failure follows the live clear");

    assert!(
        error
            .to_string()
            .contains("historical lifecycle commit failed")
    );
    assert_eq!(signals.injected_events.load(Ordering::SeqCst), 1);
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    let polls = signals.live_polls.load(Ordering::SeqCst);
    assert!(
        hybrid
            .next_batch()
            .await
            .expect_err("an uncertified event gap cannot return to Live")
            .to_string()
            .contains("poisoned")
    );
    assert_eq!(signals.live_polls.load(Ordering::SeqCst), polls);
}

#[tokio::test]
async fn post_coverage_existing_owner_remove_and_replace_fail_closed_on_the_gap() {
    for replace in [false, true] {
        let owner = HandlerId::new(if replace {
            "replaced-owner"
        } else {
            "removed-owner"
        });
        let (mut hybrid, signals) =
            covered_gap_mutation_hybrid(Some(vec![(owner.clone(), vec![portable_log_interest()])]))
                .await;
        signals.fail_historical.store(true, Ordering::SeqCst);

        let error = if replace {
            hybrid
                .add_interest_owner(owner, &[alternate_log_interest()])
                .await
                .expect_err("historical replacement fails after live changed")
        } else {
            hybrid
                .remove_interest_owner(&owner)
                .await
                .expect_err("historical removal fails after live changed")
        };

        assert!(
            error
                .to_string()
                .contains("historical lifecycle commit failed"),
            "unexpected lifecycle error: {error}"
        );
        assert_eq!(signals.injected_events.load(Ordering::SeqCst), 1);
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
        assert!(
            hybrid
                .next_batch()
                .await
                .expect_err("ordinary compensation cannot certify the lost interval")
                .to_string()
                .contains("poisoned")
        );
    }
}

#[tokio::test]
async fn post_coverage_bulk_existing_owner_failure_poisons_on_the_gap() {
    let owner = HandlerId::new("bulk-existing-owner");
    let (mut hybrid, signals) =
        covered_gap_mutation_hybrid(Some(vec![(owner.clone(), vec![portable_log_interest()])]))
            .await;
    signals.fail_historical.store(true, Ordering::SeqCst);

    let error = hybrid
        .upsert_interest_owners(vec![(owner, vec![alternate_log_interest()])])
        .await
        .expect_err("historical bulk commit fails after live replacement");

    assert!(
        error
            .to_string()
            .contains("historical lifecycle commit failed")
    );
    assert_eq!(signals.injected_events.load(Ordering::SeqCst), 1);
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn cancelled_existing_owner_mutation_poisons_when_reconciliation_exposes_a_gap() {
    let owner = HandlerId::new("cancelled-existing-owner");
    let (mut hybrid, signals) =
        covered_gap_mutation_hybrid(Some(vec![(owner.clone(), vec![portable_log_interest()])]))
            .await;
    signals.block_historical.store(true, Ordering::SeqCst);

    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            hybrid.add_interest_owner(owner.clone(), &[alternate_log_interest()]),
        )
        .await
        .is_err(),
        "historical commit remains uncertain after the live mutation"
    );
    assert_eq!(signals.injected_events.load(Ordering::SeqCst), 1);

    let error = hybrid
        .add_interest_owner(owner, &[alternate_log_interest()])
        .await
        .expect_err("retry must reconcile and classify the uncovered interval");
    assert!(error.to_string().contains("gap"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn new_or_previously_empty_owner_failure_remains_safely_retryable() {
    for previous_empty in [false, true] {
        let existing = HandlerId::new("still-active-owner");
        let owner = HandlerId::new(if previous_empty {
            "previously-empty-owner"
        } else {
            "brand-new-owner"
        });
        let mut seed = vec![(existing, vec![portable_log_interest()])];
        if previous_empty {
            seed.push((owner.clone(), Vec::new()));
        }
        let (mut hybrid, signals) = covered_gap_mutation_hybrid(Some(seed)).await;
        let interests = vec![alternate_log_interest()];
        signals.fail_historical.store(true, Ordering::SeqCst);
        hybrid
            .add_interest_owner(owner.clone(), &interests)
            .await
            .expect_err("historical activation failure is compensated");
        assert_eq!(hybrid.phase(), HybridPhase::Live);
        assert!(hybrid.poison_reason().is_none());
        hybrid
            .add_interest_owner(owner, &interests)
            .await
            .expect("safe rollback remains usable");
        assert_ne!(hybrid.phase(), HybridPhase::Poisoned);
        assert_eq!(signals.injected_events.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn cancelled_registration_is_reconciled_before_a_retry_can_mutate_again() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = BlockingRegistrationSubscriber {
        name: "history",
        registrations: Arc::clone(&registrations),
    };
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");

    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            hybrid.register_interests(&[portable_log_interest()])
        )
        .await
        .is_err(),
        "the historical commit remains pending"
    );
    assert_eq!(
        *registrations.lock().expect("registrations"),
        vec!["live", "history"]
    );

    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            hybrid.register_interests(&[portable_log_interest()])
        )
        .await
        .is_err(),
        "the retried historical commit remains pending"
    );
    assert_eq!(
        *registrations.lock().expect("registrations"),
        vec!["live", "history", "history"],
        "the retry must reconcile the uncertain historical side before touching live again"
    );
}

#[tokio::test]
async fn live_buffer_overflow_permanently_fails_closed() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(200),
            batch(&[104], b"h104"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, tokenless_batch(&[105])),
            Step::Batch(Duration::ZERO, tokenless_batch(&[106])),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_buffered_live_batches = 1;
    let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let overflow = hybrid.next_batch().await.expect_err("buffer overflow");
    assert!(overflow.to_string().contains("live buffer exceeded"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    assert!(
        hybrid
            .next_batch()
            .await
            .expect_err("poisoned coordinator")
            .to_string()
            .contains("poisoned")
    );
}

#[tokio::test]
async fn ack_gated_live_replay_is_buffered_only_once_during_catchup() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(50),
            batch(&[104], b"history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[105], b"same-live-token")),
            Step::Batch(Duration::ZERO, batch(&[105], b"same-live-token")),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_buffered_live_batches = 1;
    let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let historical = hybrid
        .next_batch()
        .await
        .expect("historical result")
        .expect("historical batch");
    assert_eq!(hybrid.buffered_live_batches(), 1);
    hybrid
        .acknowledge_delivery(historical.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(live.records().len(), 1);
}

#[tokio::test]
async fn one_live_token_cannot_hide_changed_exact_context() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(100),
                batch(&[104], b"history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let original = batch(&[105], b"same-live-token");
        let mut changed_record = record(105);
        changed_record.context.source = InputSource::Poll;
        changed_record.context.chain_status = ChainStatus::Included {
            block: block_ref(105),
            confirmations: 7,
        };
        let changed = ReactiveInputBatch::new(vec![changed_record])
            .with_delivery_token(SubscriberDeliveryToken::new(b"same-live-token".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, original),
                Step::Batch(Duration::ZERO, changed),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();

        let historical = hybrid
            .next_batch()
            .await
            .expect("historical result")
            .expect("historical delivery");
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .expect("acknowledge historical delivery");
        let original = hybrid
            .next_batch()
            .await
            .expect("original live result")
            .expect("original live delivery");
        hybrid
            .acknowledge_delivery(original.delivery_token().unwrap().clone())
            .await
            .expect("acknowledge original live delivery");
        let error = hybrid
            .next_batch()
            .await
            .expect_err("same raw token changed exact provenance context");
        assert!(
            error.to_string().contains("different data"),
            "unexpected exact-context error: {error}"
        );
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("exact-context replay regression timed out");
}

#[tokio::test]
async fn one_live_token_cannot_hide_changed_payload_with_the_same_input_identity() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(20),
            batch(&[104], b"history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let changed = ReactiveInputBatch::new(vec![record_with_data(105, 32)])
        .with_delivery_token(SubscriberDeliveryToken::new(b"same-live-token".to_vec()));
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[105], b"same-live-token")),
            Step::Batch(Duration::ZERO, changed),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let historical = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(historical.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let first_live = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(first_live.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let error = hybrid
        .next_batch()
        .await
        .expect_err("source reused committed token");
    assert!(
        error.to_string().contains("conflicting payload")
            || error.to_string().contains("reused one delivery token")
            || error.to_string().contains("different data")
    );
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn acknowledgeable_live_batch_is_not_repolled_while_buffered() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(30),
            batch(&[104], b"history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let polls = Arc::new(Mutex::new(0));
    let live = ReplayUntilAcknowledgedSubscriber {
        batch: batch(&[105], b"durable-live"),
        polls: Arc::clone(&polls),
    };
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    hybrid
        .next_batch()
        .await
        .expect("history result")
        .expect("history batch");
    assert_eq!(*polls.lock().expect("poll counter"), 1);
    assert_eq!(hybrid.buffered_live_batches(), 1);
}

#[tokio::test]
async fn lifecycle_change_waits_for_buffered_acknowledgeable_live_delivery() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(20),
            batch(&[104], b"history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, batch(&[105], b"durable-live"))],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");
    let historical = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(historical.delivery_token().unwrap().clone())
        .await
        .unwrap();
    assert_eq!(hybrid.phase(), HybridPhase::DrainingLive);
    assert_eq!(hybrid.buffered_live_batches(), 1);

    let error = hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect_err("buffered durable delivery blocks lifecycle changes");
    assert!(error.to_string().contains("any buffered live delivery"));
    assert_eq!(registrations.lock().expect("registrations").len(), 2);
}

#[tokio::test]
async fn historical_source_must_tokenize_every_delivery_it_claims_is_durable() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[99]))],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::from_secs(1), tokenless_batch(&[100]))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let error = hybrid.next_batch().await.expect_err("untokenized history");
    assert!(error.to_string().contains("durable historical delivery"));
}

#[tokio::test]
async fn one_large_live_batch_is_rejected_by_record_bound() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(100),
            batch(&[104], b"history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(
            Duration::ZERO,
            batch(&[105, 106], b"large-live"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_buffered_live_records = 1;
    let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let error = hybrid.next_batch().await.expect_err("record bound");
    assert!(error.to_string().contains("ingress record bound (2/1)"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn one_large_live_batch_is_rejected_by_accounted_byte_bound() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(100),
            batch(&[104], b"history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live_batch = ReactiveInputBatch::new(vec![record_with_data(105, 2_048)])
        .with_delivery_token(SubscriberDeliveryToken::new(b"large-live".to_vec()));
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, live_batch)],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_buffered_live_bytes = 1_024;
    let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .expect("register");

    let error = hybrid.next_batch().await.expect_err("byte bound");
    assert!(error.to_string().contains("per-batch ingress byte bound"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn conflicting_cross_source_payload_is_never_silently_suppressed() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[104], b"history-conflict"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::ZERO,
                ReactiveInputBatch::new(vec![record_with_data(104, 32)])
                    .with_delivery_token(SubscriberDeliveryToken::new(b"live-conflict".to_vec())),
            )],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let historical = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let error = hybrid.next_batch().await.expect_err("payload conflict");
        assert!(error.to_string().contains("conflicting payload"));
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("conflict regression timed out");
}

#[tokio::test]
async fn overlap_accepts_optional_metadata_enrichment_in_either_source() {
    tokio::time::timeout(Duration::from_secs(2), async {
        for history_is_richer in [false, true] {
            let registrations = Arc::new(Mutex::new(Vec::new()));
            let sparse = record_with_block_metadata(104, None, None);
            let rich = record(104);
            let (history_record, live_record) = if history_is_richer {
                (rich, sparse)
            } else {
                (sparse, rich)
            };
            let history = ScriptedSubscriber::new(
                "history",
                [Step::Batch(
                    Duration::from_millis(20),
                    ReactiveInputBatch::new(vec![history_record]).with_delivery_token(
                        SubscriberDeliveryToken::new(b"history-meta".to_vec()),
                    ),
                )],
                Arc::new(Mutex::new(Vec::new())),
                Arc::clone(&registrations),
            );
            let live = ScriptedSubscriber::new(
                "live",
                [Step::Batch(
                    Duration::ZERO,
                    ReactiveInputBatch::new(vec![live_record])
                        .with_delivery_token(SubscriberDeliveryToken::new(b"live-meta".to_vec())),
                )],
                Arc::new(Mutex::new(Vec::new())),
                registrations,
            );
            let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
            hybrid
                .register_interests(&[portable_log_interest()])
                .await
                .unwrap();
            let historical = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(historical.delivery_token().unwrap().clone())
                .await
                .unwrap();
            let overlap = hybrid.next_batch().await.unwrap().unwrap();
            assert!(overlap.records().is_empty());
            hybrid
                .acknowledge_delivery(overlap.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
    })
    .await
    .expect("metadata enrichment regression timed out");
}

#[tokio::test]
async fn same_hash_with_conflicting_parent_metadata_fails_closed() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[104], b"history-parent"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let conflicting = record_with_block_metadata(
            104,
            Some(B256::repeat_byte(0xfe)),
            block_ref(104).timestamp,
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::ZERO,
                ReactiveInputBatch::new(vec![conflicting])
                    .with_delivery_token(SubscriberDeliveryToken::new(b"live-parent".to_vec())),
            )],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let historical = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let error = hybrid.next_batch().await.expect_err("metadata conflict");
        assert!(error.to_string().contains("metadata") || error.to_string().contains("parent"));
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("metadata conflict regression timed out");
}

#[tokio::test]
async fn overlap_outside_retained_history_requires_full_resynchronization() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[104], b"history-window"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, batch(&[103], b"live-old"))],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let historical = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let error = hybrid.next_batch().await.expect_err("unverifiable overlap");
        assert!(
            error
                .to_string()
                .contains("outside retained hybrid history")
        );
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("old-overlap regression timed out");
}

#[tokio::test]
async fn restored_recovery_outside_the_payload_witness_window_fails_closed() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[100], b"history-100"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                Step::Batch(Duration::from_millis(30), batch(&[102], b"live-102")),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut config = HybridConfig::default();
        config.recent_input_capacity = 1;
        let mut first = HybridSubscriber::new(history, live, config).unwrap();
        first
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = first.next_batch().await.unwrap().unwrap();
            first
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        let committed = first.next_batch().await.unwrap().unwrap();
        let token = committed.delivery_token().unwrap().clone();
        let checkpoint = committed.subscriber_checkpoint().unwrap().clone();
        first.acknowledge_delivery(token.clone()).await.unwrap();

        let historical_acks = Arc::new(Mutex::new(Vec::new()));
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[101, 102], b"history-recovery"),
            )],
            Arc::clone(&historical_acks),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, tokenless_batch(&[103]))],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut restored = HybridSubscriber::new(history, live, config).unwrap();
        let position = resume_position(
            102,
            vec![block_ref(100), block_ref(101), block_ref(102)],
            token,
            checkpoint,
        );
        restored
            .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();

        let error = restored
            .next_batch()
            .await
            .expect_err("unverifiable historical overlap");
        assert!(error.to_string().contains("payload-witness window"));
        assert_eq!(restored.phase(), HybridPhase::Poisoned);
        assert_eq!(
            *historical_acks.lock().unwrap(),
            vec![b"history-100".to_vec()],
            "restore retries only the already-persisted historical ACK; the unverifiable recovery page remains unacknowledged"
        );
    })
    .await
    .expect("restored witness-window regression timed out");
}

#[tokio::test]
async fn tokenless_child_delivery_clears_stale_source_token_and_checkpoint() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[104], b"history-clear"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let first_live = batch(&[105], b"live-old")
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"live-old-cp".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, first_live),
                Step::Batch(Duration::from_millis(10), tokenless_batch(&[106])),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let historical = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let first_live = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(first_live.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let tokenless = hybrid.next_batch().await.unwrap().unwrap();
        let token = tokenless.delivery_token().unwrap().clone();
        let checkpoint = tokenless.subscriber_checkpoint().unwrap().clone();
        hybrid.acknowledge_delivery(token.clone()).await.unwrap();

        let history_restores = Arc::new(Mutex::new(Vec::new()));
        let live_restores = Arc::new(Mutex::new(Vec::new()));
        let history =
            RestoreProbeSubscriber::new(true, Some(1), [Ok(())], Arc::clone(&history_restores));
        let live =
            RestoreProbeSubscriber::new(false, Some(1), [Ok(())], Arc::clone(&live_restores));
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        let position = SubscriberResumePosition::new(
            1,
            block_ref(106),
            vec![block_ref(104), block_ref(105), block_ref(106)],
            Some(token),
            Some(checkpoint),
        );
        restored
            .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();
        let live_resume = live_restores.lock().unwrap().last().unwrap().clone();
        assert!(live_resume.delivery_token.is_none());
        assert!(live_resume.subscriber_checkpoint.is_none());
    })
    .await
    .expect("stale-source-position regression timed out");
}

#[tokio::test]
async fn finality_only_delivery_does_not_advance_live_event_coverage_on_restart() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[104], b"history-finality"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let safe = ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Finalized(block_ref(105))])
            .with_delivery_token(SubscriberDeliveryToken::new(b"finalized-105".to_vec()))
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"finalized-cp".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[105], b"live-105")),
                Step::Batch(Duration::from_millis(10), safe),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let historical = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let live = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(live.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let finalized = hybrid.next_batch().await.unwrap().unwrap();
        let token = finalized.delivery_token().unwrap().clone();
        let checkpoint = finalized.subscriber_checkpoint().unwrap().clone();
        hybrid.acknowledge_delivery(token.clone()).await.unwrap();

        let history_restores = Arc::new(Mutex::new(Vec::new()));
        let live_restores = Arc::new(Mutex::new(Vec::new()));
        let mut restored = HybridSubscriber::new(
            RestoreProbeSubscriber::new(true, Some(1), [Ok(())], history_restores),
            RestoreProbeSubscriber::new(false, Some(1), [Ok(())], Arc::clone(&live_restores)),
            HybridConfig::default(),
        )
        .unwrap();
        let position = SubscriberResumePosition::new(
            1,
            block_ref(105),
            vec![block_ref(104), block_ref(105)],
            Some(token),
            Some(checkpoint),
        );
        restored
            .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();
        let live_resume = live_restores.lock().unwrap().last().unwrap().clone();
        assert_eq!(live_resume.coverage_head.number, 105);
        assert_eq!(
            live_resume.delivery_token.as_ref().unwrap().as_bytes(),
            b"finalized-105"
        );
        assert_eq!(
            live_resume
                .subscriber_checkpoint
                .as_ref()
                .unwrap()
                .as_bytes(),
            b"finalized-cp"
        );
    })
    .await
    .expect("finality coverage regression timed out");
}

#[tokio::test]
async fn acknowledgement_is_idempotent_without_double_forwarding() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let history_acks = Arc::new(Mutex::new(Vec::new()));
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::ZERO, batch(&[104], b"idempotent"))],
            Arc::clone(&history_acks),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[105], b"live"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        let token = delivery.delivery_token().unwrap().clone();
        hybrid.acknowledge_delivery(token.clone()).await.unwrap();
        hybrid.acknowledge_delivery(token).await.unwrap();
        assert_eq!(*history_acks.lock().unwrap(), vec![b"idempotent".to_vec()]);
    })
    .await
    .expect("idempotent ACK regression timed out");
}

#[tokio::test]
async fn partial_child_restore_requires_exact_retry_and_blocks_other_operations() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let (token, checkpoint) = synthetic_resume_fixture().await;
        let position = SubscriberResumePosition::new(
            1,
            block_ref(105),
            vec![block_ref(104), block_ref(105)],
            Some(token.clone()),
            Some(checkpoint),
        );
        let history_restores = Arc::new(Mutex::new(Vec::new()));
        let live_restores = Arc::new(Mutex::new(Vec::new()));
        let mut hybrid = HybridSubscriber::new(
            RestoreProbeSubscriber::new(true, Some(1), [Ok(())], Arc::clone(&history_restores)),
            RestoreProbeSubscriber::new(
                false,
                Some(1),
                [Err("fail first"), Ok(())],
                Arc::clone(&live_restores),
            ),
            HybridConfig::default(),
        )
        .unwrap();
        hybrid
            .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
            .await
            .unwrap();
        assert!(hybrid.restore_position(&position).is_err());
        assert!(
            hybrid
                .next_batch()
                .await
                .unwrap_err()
                .to_string()
                .contains("restore")
        );
        assert!(
            hybrid
                .acknowledge_delivery(token.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("restore")
        );
        let mut different = position.clone();
        different.coverage_head.timestamp = None;
        assert!(
            hybrid
                .restore_position(&different)
                .unwrap_err()
                .to_string()
                .contains("different position")
        );
        hybrid.restore_position(&position).unwrap();
        assert_eq!(history_restores.lock().unwrap().len(), 1);
        assert_eq!(live_restores.lock().unwrap().len(), 2);
        hybrid.acknowledge_delivery(token).await.unwrap();
    })
    .await
    .expect("partial restore regression timed out");
}

#[tokio::test]
async fn restored_child_ack_is_retried_before_polling_and_post_ack_reuse_is_rejected() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let original_commitment = SubscriberPayloadCommitment::new(B256::repeat_byte(0x41));
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let child_delivery = batch(&[104], b"replay-token")
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"replay-cursor".to_vec()))
            .with_payload_commitment(original_commitment);
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::ZERO, child_delivery.clone())],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::from_millis(50),
                batch(&[105], b"live"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut first = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        first
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let committed = first.next_batch().await.unwrap().unwrap();
        assert_eq!(committed.payload_commitment(), Some(&original_commitment));
        let token = committed.delivery_token().unwrap().clone();
        let checkpoint = committed.subscriber_checkpoint().unwrap().clone();
        first.acknowledge_delivery(token.clone()).await.unwrap();
        let position = SubscriberResumePosition::new(
            1,
            block_ref(104),
            vec![block_ref(104)],
            Some(token),
            Some(checkpoint),
        );

        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history_acks = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::ZERO, child_delivery)],
            Arc::clone(&history_acks),
            Arc::clone(&registrations),
        );
        let live =
            ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), registrations);
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        restored
            .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();
        let error = restored.next_batch().await.unwrap_err();
        assert!(error.to_string().contains("reused one delivery token"));
        assert_eq!(
            *history_acks.lock().unwrap(),
            vec![b"replay-token".to_vec()]
        );
        assert_eq!(restored.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("restored replay regression timed out");
}

#[tokio::test]
async fn durable_live_restore_retries_the_child_ack_without_reusing_the_outer_token() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"history-100"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let first_live_acks = Arc::new(Mutex::new(Vec::new()));
        let live = DurableAckReplaySubscriber {
            historical: false,
            in_flight: None,
            next: VecDeque::from([batch(&[101], b"durable-live-101")]),
            acknowledgements: Arc::clone(&first_live_acks),
        };
        let mut first = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        first
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let historical = first.next_batch().await.unwrap().unwrap();
        first
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let committed_live = first.next_batch().await.unwrap().unwrap();
        let outer_token = committed_live.delivery_token().unwrap().clone();
        let checkpoint = committed_live.subscriber_checkpoint().unwrap().clone();
        first
            .acknowledge_delivery(outer_token.clone())
            .await
            .unwrap();
        assert_eq!(
            *first_live_acks.lock().unwrap(),
            vec![b"durable-live-101".to_vec()]
        );
        let position = resume_position(
            101,
            vec![block_ref(100), block_ref(101)],
            outer_token.clone(),
            checkpoint,
        );
        drop(first);

        let history =
            RestoreProbeSubscriber::new(true, Some(1), [Ok(())], Arc::new(Mutex::new(Vec::new())));
        let replayed_live_acks = Arc::new(Mutex::new(Vec::new()));
        let next_record = record_for_block(block_ref(101), B256::repeat_byte(0xdd));
        let next_live = ReactiveInputBatch::new(vec![next_record])
            .with_delivery_token(SubscriberDeliveryToken::new(b"durable-live-next".to_vec()));
        let live = DurableAckReplaySubscriber {
            historical: false,
            in_flight: Some(batch(&[101], b"durable-live-101")),
            next: VecDeque::from([next_live]),
            acknowledgements: Arc::clone(&replayed_live_acks),
        };
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        restored
            .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();

        let next = restored.next_batch().await.unwrap().unwrap();
        assert_eq!(next.records().len(), 1);
        assert_ne!(next.delivery_token(), Some(&outer_token));
        assert_eq!(
            *replayed_live_acks.lock().unwrap(),
            vec![b"durable-live-101".to_vec()],
            "restore must retry the already-persisted child ACK before polling"
        );
    })
    .await
    .expect("durable-live replay regression timed out");
}

#[tokio::test]
async fn untokenized_live_buffer_blocks_reconfiguration_after_history_error() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Error(Duration::from_millis(20), "history failed")],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, tokenless_batch(&[105]))],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        hybrid.next_batch().await.expect_err("history failure");
        assert_eq!(hybrid.buffered_live_batches(), 1);
        let before = registrations.lock().unwrap().len();
        let error = hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("any buffered live delivery"));
        assert_eq!(registrations.lock().unwrap().len(), before);
    })
    .await
    .expect("untokenized buffer regression timed out");
}

#[tokio::test]
async fn full_block_bodies_are_rejected_instead_of_collapsed_by_header_hash() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                ReactiveInputBatch::new(header_and_full_block_records(104))
                    .with_delivery_token(SubscriberDeliveryToken::new(b"representations".to_vec())),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::ZERO,
                batch(&[105], b"live-representations"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let error = hybrid.next_batch().await.unwrap_err();
        assert!(error.to_string().contains("does not accept full blocks"));
    })
    .await
    .expect("representation regression timed out");
}

#[tokio::test]
async fn block_header_without_payload_commitment_is_rejected_before_delivery_or_ack() {
    let history_acks = Arc::new(Mutex::new(Vec::new()));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let header = ReactiveInputBatch::new(vec![header_record_with_gas_limit(104, 30_000_000)])
        .with_delivery_token(SubscriberDeliveryToken::new(b"uncommitted-header".to_vec()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::from_millis(10), header)],
        Arc::clone(&history_acks),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[105]))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let error = hybrid
        .next_batch()
        .await
        .expect_err("an outer token cannot witness an uncommitted header body");
    assert!(error.to_string().contains("payload commitment"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    assert!(history_acks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn restored_header_replay_with_altered_body_commitment_fails_closed() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let original = ReactiveInputBatch::new(vec![header_record_with_gas_limit(104, 30_000_000)])
            .with_delivery_token(SubscriberDeliveryToken::new(b"header-replay".to_vec()))
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"header-cursor".to_vec()))
            .with_payload_commitment(SubscriberPayloadCommitment::new(B256::repeat_byte(0xa1)));
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::ZERO, original)],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::from_millis(50),
                tokenless_batch(&[105]),
            )],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut first = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        first
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let committed = first.next_batch().await.unwrap().unwrap();
        let token = committed.delivery_token().unwrap().clone();
        let checkpoint = committed.subscriber_checkpoint().unwrap().clone();
        first.acknowledge_delivery(token.clone()).await.unwrap();
        let position = SubscriberResumePosition::new(
            1,
            block_ref(104),
            vec![block_ref(104)],
            Some(token),
            Some(checkpoint),
        );

        let altered = ReactiveInputBatch::new(vec![header_record_with_gas_limit(104, 31_000_000)])
            .with_delivery_token(SubscriberDeliveryToken::new(b"header-replay".to_vec()))
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(b"header-cursor".to_vec()))
            .with_payload_commitment(SubscriberPayloadCommitment::new(B256::repeat_byte(0xa2)));
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::ZERO, altered)],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live =
            ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), registrations);
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        restored
            .prepare_restore_base_lifecycle(&position, &[portable_log_interest()])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();
        let error = restored.next_batch().await.unwrap_err();
        assert!(
            error.to_string().contains("changed data")
                || error.to_string().contains("conflicting payload"),
            "unexpected header replay error: {error}"
        );
        assert_eq!(restored.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("header replay regression timed out");
}

#[tokio::test]
async fn oversized_child_batch_is_rejected_before_record_witness_construction() {
    let history_acks = Arc::new(Mutex::new(Vec::new()));
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let mut oversized = record_with_data(104, 4_096);
    oversized.context.chain_id = Some(2);
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::ZERO,
            ReactiveInputBatch::new(vec![oversized])
                .with_chain_id(1)
                .with_delivery_token(SubscriberDeliveryToken::new(b"oversized".to_vec())),
        )],
        Arc::clone(&history_acks),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), registrations);
    let mut config = HybridConfig::default();
    config.max_buffered_live_bytes = 1_024;
    let mut hybrid = HybridSubscriber::new(history, live, config).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let error = hybrid.next_batch().await.unwrap_err();
    assert!(error.to_string().contains("per-batch ingress byte bound"));
    assert!(
        !error.to_string().contains("source record has chain"),
        "resource preflight must run before validation builds payload witnesses"
    );
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    assert!(history_acks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn empty_child_envelope_is_rejected_before_live_buffering() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(100),
            batch(&[100], b"empty-envelope-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let empty = || ReactiveInputBatch::new(Vec::new()).with_chain_id(1);
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, empty()),
            Step::Batch(Duration::ZERO, empty()),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_buffered_live_batches = 1;
    let mut hybrid = HybridSubscriber::new(history, live, config).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let error = hybrid
        .next_batch()
        .await
        .expect_err("empty Some(batch) is a child protocol violation");

    assert!(
        error.to_string().contains("empty child batch"),
        "unexpected empty-envelope error: {error}"
    );
    assert_eq!(hybrid.buffered_live_batches(), 0);
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn empty_child_envelope_is_rejected_from_historical_delivery() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::ZERO,
            ReactiveInputBatch::new(Vec::new()).with_chain_id(1),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(
            Duration::from_secs(1),
            batch(&[101], b"historical-empty-live"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let error = hybrid
        .next_batch()
        .await
        .expect_err("empty historical envelope");
    assert!(error.to_string().contains("empty child batch"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn empty_child_envelope_is_rejected_in_live_phase() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"live-empty-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[101], b"live-empty-seed")),
            Step::Batch(
                Duration::ZERO,
                ReactiveInputBatch::new(Vec::new()).with_chain_id(1),
            ),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();
    for _ in 0..2 {
        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
            .await
            .unwrap();
    }
    assert_eq!(hybrid.phase(), HybridPhase::Live);

    let error = hybrid.next_batch().await.expect_err("empty live envelope");
    assert!(error.to_string().contains("empty child batch"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn forwarded_token_without_source_coverage_is_rejected_before_output() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let blockless = ReactiveInputBatch::new(Vec::new())
        .with_chain_id(1)
        .with_chain_controls([ChainControl::Barrier {
            id: b"blockless-forwarded-token".to_vec(),
            block: None,
        }])
        .with_delivery_token(SubscriberDeliveryToken::new(
            b"blockless-forwarded-token".to_vec(),
        ))
        .with_subscriber_checkpoint(SubscriberCheckpoint::new(
            b"blockless-source-position".to_vec(),
        ));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::ZERO, blockless)],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), registrations);
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let error = hybrid
        .next_batch()
        .await
        .expect_err("forwarded durable token needs restorable source coverage");
    assert!(
        error
            .to_string()
            .contains("without restorable canonical coverage"),
        "unexpected headless-source error: {error}"
    );
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn implicit_all_audiences_are_bounded_by_projected_owner_work_before_filtering() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"projected-owner-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[101], b"projected-owner-live")),
            Step::Batch(
                Duration::ZERO,
                batch(&[102, 103], b"projected-owner-overflow"),
            ),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_recent_owner_entries = 2;
    let mut hybrid = HybridSubscriber::new(history, live, config).unwrap();
    hybrid
        .upsert_interest_owners(vec![
            (HandlerId::new("projected-a"), vec![portable_log_interest()]),
            (HandlerId::new("projected-b"), vec![portable_log_interest()]),
        ])
        .await
        .unwrap();
    for _ in 0..2 {
        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
            .await
            .unwrap();
    }
    assert_eq!(hybrid.phase(), HybridPhase::Live);

    let error = hybrid
        .next_batch()
        .await
        .expect_err("two all-owner records exceed projected owner-work budget");
    assert!(
        error.to_string().contains("projected owner associations"),
        "unexpected projected-owner error: {error}"
    );
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn all_except_audiences_include_installed_owner_expansion_in_ingress_budget() {
    let owner_a = HandlerId::new("all-except-a");
    let owner_b = HandlerId::new("all-except-b");
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"all-except-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let overflow = routed_batch(
        &[102],
        DeliveryAudience::AllExcept(vec![owner_a.clone()]),
        b"all-except-overflow",
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[101], b"all-except-live")),
            Step::Batch(Duration::ZERO, overflow),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_recent_owner_entries = 2;
    let mut hybrid = HybridSubscriber::new(history, live, config).unwrap();
    hybrid
        .upsert_interest_owners(vec![
            (owner_a, vec![portable_log_interest()]),
            (owner_b, vec![portable_log_interest()]),
        ])
        .await
        .unwrap();
    for _ in 0..2 {
        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
            .await
            .unwrap();
    }

    let error = hybrid
        .next_batch()
        .await
        .expect_err("projected AllExcept work");
    assert!(error.to_string().contains("projected owner associations"));
}

#[tokio::test]
async fn explicit_owner_audience_is_bounded_before_id_validation_or_transcript_work() {
    let owner_a = HandlerId::new("explicit-a");
    let owner_b = HandlerId::new("explicit-b");
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"explicit-owner-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let overflow = routed_batch(
        &[102],
        DeliveryAudience::Owners(vec![
            owner_a.clone(),
            owner_b.clone(),
            HandlerId::new("explicit-unbounded-unknown"),
        ]),
        b"explicit-owner-overflow",
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[101], b"explicit-owner-live")),
            Step::Batch(Duration::ZERO, overflow),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut config = HybridConfig::default();
    config.max_recent_owner_entries = 2;
    let mut hybrid = HybridSubscriber::new(history, live, config).unwrap();
    hybrid
        .upsert_interest_owners(vec![
            (owner_a, vec![portable_log_interest()]),
            (owner_b, vec![portable_log_interest()]),
        ])
        .await
        .unwrap();
    for _ in 0..2 {
        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
            .await
            .unwrap();
    }

    let error = hybrid.next_batch().await.expect_err("explicit owner bound");
    assert!(
        error
            .to_string()
            .contains("explicit owner-audience ingress bound")
    );
}

#[tokio::test]
async fn child_delivery_to_an_unknown_owner_is_rejected_before_output() {
    let installed = HandlerId::new("installed-owner");
    let unknown = HandlerId::new("unknown-owner");
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"unknown-owner-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let misrouted = routed_batch(
        &[102],
        DeliveryAudience::Owners(vec![unknown]),
        b"unknown-owner-delivery",
    );
    let live = ScriptedSubscriber::new(
        "live",
        [
            Step::Batch(Duration::ZERO, batch(&[101], b"unknown-owner-live")),
            Step::Batch(Duration::ZERO, misrouted),
        ],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .upsert_interest_owners(vec![(installed, vec![portable_log_interest()])])
        .await
        .unwrap();
    for _ in 0..2 {
        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
            .await
            .unwrap();
    }

    let error = hybrid
        .next_batch()
        .await
        .expect_err("unknown owner delivery must fail closed");
    assert!(error.to_string().contains("unknown owner"));
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn tokened_blockless_historical_barrier_is_rejected_without_acknowledgement() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let blockless = ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::Barrier {
                id: b"cursor-only".to_vec(),
                block: None,
            }])
            .with_delivery_token(SubscriberDeliveryToken::new(b"barrier-none".to_vec()));
        let history_acks = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [
                Step::Batch(Duration::from_millis(10), blockless),
                Step::Batch(Duration::from_millis(10), batch(&[104], b"history-data")),
            ],
            Arc::clone(&history_acks),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, batch(&[105], b"live-data"))],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let error = hybrid
            .next_batch()
            .await
            .expect_err("a forwarded cursor requires restorable canonical coverage");
        assert!(
            error
                .to_string()
                .contains("forwarded delivery token without restorable canonical coverage")
        );
        assert!(history_acks.lock().unwrap().is_empty());
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("blockless barrier regression timed out");
}

#[tokio::test]
async fn second_owner_revision_is_rejected_until_previous_catchup_drains() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                batch(&[104], b"owner-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, batch(&[105], b"owner-live"))],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .add_interest_owner_with_backfill(
                HandlerId::new("first"),
                &[portable_log_interest()],
                SubscriberBackfill::from_block(100),
            )
            .await
            .unwrap();
        let historical = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let error = hybrid
            .add_interest_owner_with_backfill(
                HandlerId::new("second"),
                &[portable_log_interest()],
                SubscriberBackfill::from_block(100),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("buffered live delivery")
                || error.to_string().contains("previous lifecycle revision")
        );
    })
    .await
    .expect("lifecycle revision regression timed out");
}

#[test]
fn constructor_rejects_missing_or_mismatched_source_chain_ids() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let missing_history = RestoreProbeSubscriber::new(true, None, [], Arc::clone(&calls));
    let live = RestoreProbeSubscriber::new(false, Some(1), [], Arc::clone(&calls));
    assert!(
        HybridSubscriber::new(missing_history, live, HybridConfig::default())
            .err()
            .unwrap()
            .to_string()
            .contains("authoritative chain id")
    );

    let history = RestoreProbeSubscriber::new(true, Some(1), [], Arc::clone(&calls));
    let wrong_live = RestoreProbeSubscriber::new(false, Some(2), [], calls);
    assert!(
        HybridSubscriber::new(history, wrong_live, HybridConfig::default())
            .err()
            .unwrap()
            .to_string()
            .contains("same chain")
    );
}

#[tokio::test]
async fn unresolved_live_chain_rejects_nonempty_registration_and_compensates() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let history_calls = Arc::new(Mutex::new(Vec::new()));
        let live_calls = Arc::new(Mutex::new(Vec::new()));
        let history = RestoreProbeSubscriber::new(true, Some(1), [], history_calls);
        let live = RestoreProbeSubscriber::new(false, None, [], live_calls);
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        let interest = ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new(),
            local_matcher: None,
            route_key: None,
        });
        let error = hybrid.register_interests(&[interest]).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("has not resolved required chain 1")
        );
    })
    .await
    .expect("lazy-chain registration regression timed out");
}

#[tokio::test]
async fn unresolved_live_chain_allows_effective_empty_lifecycle_revisions() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let history = RestoreProbeSubscriber::new(true, Some(1), [], Arc::clone(&calls));
        let live = RestoreProbeSubscriber::new(false, None, [], calls);
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();

        hybrid.register_interests(&[]).await.unwrap();
        let owner = HandlerId::new("empty-unresolved-owner");
        hybrid
            .upsert_interest_owners(vec![(owner.clone(), Vec::new())])
            .await
            .unwrap();
        let barrier = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(barrier.delivery_token().unwrap().clone())
            .await
            .unwrap();
        hybrid
            .replace_interest_owners_with_global_backfill(
                vec![(HandlerId::new("replacement-empty-owner"), Vec::new())],
                SubscriberBackfill::from_block(100),
            )
            .await
            .unwrap();
        let barrier = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(barrier.delivery_token().unwrap().clone())
            .await
            .unwrap();
        hybrid
            .remove_interest_owner(&HandlerId::new("replacement-empty-owner"))
            .await
            .unwrap();
        let barrier = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(barrier.delivery_token().unwrap().clone())
            .await
            .unwrap();
        assert_eq!(hybrid.phase(), HybridPhase::Live);
    })
    .await
    .expect("effective-empty unresolved-chain regression timed out");
}

#[tokio::test]
async fn effective_empty_coordinator_returns_none_without_polling_either_source() {
    let history = CapabilitySubscriber(SubscriberCapabilities::new([
        SubscriberCapability::HistoricalBackfill,
        SubscriberCapability::DurableReplay,
        SubscriberCapability::Barriers,
    ]));
    let live_polls = Arc::new(Mutex::new(0));
    let live = ReplayUntilAcknowledgedSubscriber {
        batch: tokenless_batch(&[100]),
        polls: Arc::clone(&live_polls),
    };
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();

    let delivery = tokio::time::timeout(Duration::from_millis(100), hybrid.next_batch())
        .await
        .expect("effective-empty polling must return promptly")
        .expect("effective-empty polling is valid");

    assert!(delivery.is_none());
    assert_eq!(*live_polls.lock().unwrap(), 0);
    assert_eq!(hybrid.phase(), HybridPhase::Live);
}

#[tokio::test]
async fn clearing_the_last_interest_emits_one_replayable_barrier_then_stops_polling_both_sources() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let historical_polls = Arc::new(AtomicUsize::new(0));
    let live_polls = Arc::new(AtomicUsize::new(0));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(
            Duration::from_millis(10),
            batch(&[100], b"effective-empty-history"),
        )],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    )
    .with_poll_counter(Arc::clone(&historical_polls));
    let live = ScriptedSubscriber::new(
        "live",
        [Step::Batch(Duration::ZERO, tokenless_batch(&[101]))],
        Arc::new(Mutex::new(Vec::new())),
        registrations,
    )
    .with_poll_counter(Arc::clone(&live_polls));
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();

    let historical = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(historical.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let live = hybrid.next_batch().await.unwrap().unwrap();
    hybrid
        .acknowledge_delivery(live.delivery_token().unwrap().clone())
        .await
        .unwrap();
    assert_eq!(hybrid.phase(), HybridPhase::Live);

    hybrid.register_interests(&[]).await.unwrap();
    let historical_before = historical_polls.load(Ordering::SeqCst);
    let live_before = live_polls.load(Ordering::SeqCst);
    let barrier = tokio::time::timeout(Duration::from_millis(100), hybrid.next_batch())
        .await
        .expect("post-clear polling must return promptly")
        .expect("post-clear polling is valid")
        .expect("the lifecycle revision must be durably observable");
    assert!(barrier.records().is_empty());
    assert!(matches!(
        barrier.chain_controls(),
        [ChainControl::Barrier { block: Some(block), .. }] if block.number == 101
    ));
    assert!(barrier.subscriber_checkpoint().is_some());
    assert!(barrier.delivery_token().is_some());

    let replay = hybrid.next_batch().await.unwrap().unwrap();
    assert_eq!(replay.chain_controls(), barrier.chain_controls());
    assert_eq!(replay.delivery_token(), barrier.delivery_token());
    assert_eq!(
        replay.subscriber_checkpoint(),
        barrier.subscriber_checkpoint()
    );
    assert_eq!(historical_polls.load(Ordering::SeqCst), historical_before);
    assert_eq!(live_polls.load(Ordering::SeqCst), live_before);

    hybrid
        .acknowledge_delivery(barrier.delivery_token().unwrap().clone())
        .await
        .unwrap();
    let delivery = hybrid.next_batch().await.unwrap();

    assert!(delivery.is_none());
    assert_eq!(historical_polls.load(Ordering::SeqCst), historical_before);
    assert_eq!(live_polls.load(Ordering::SeqCst), live_before);
    assert_eq!(hybrid.phase(), HybridPhase::Live);
}

#[tokio::test]
async fn clear_barrier_preserves_durable_live_cursor_across_restore_then_idles() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let historical_polls = Arc::new(AtomicUsize::new(0));
        let live_polls = Arc::new(AtomicUsize::new(0));
        let history = DurableLifecycleSubscriber {
            historical: true,
            steps: VecDeque::from([Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"durable-empty-history-100").with_subscriber_checkpoint(
                    SubscriberCheckpoint::new(b"durable-empty-history-cursor".to_vec()),
                ),
            )]),
            polls: Arc::clone(&historical_polls),
            restores: Arc::new(Mutex::new(Vec::new())),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
        };
        let live = DurableLifecycleSubscriber {
            historical: false,
            steps: VecDeque::from([Step::Batch(
                Duration::ZERO,
                batch(&[101], b"durable-empty-live-101").with_subscriber_checkpoint(
                    SubscriberCheckpoint::new(b"durable-empty-live-cursor".to_vec()),
                ),
            )]),
            polls: Arc::clone(&live_polls),
            restores: Arc::new(Mutex::new(Vec::new())),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
        };
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        assert_eq!(hybrid.phase(), HybridPhase::Live);

        hybrid.register_interests(&[]).await.unwrap();
        let historical_before = historical_polls.load(Ordering::SeqCst);
        let live_before = live_polls.load(Ordering::SeqCst);
        let barrier = hybrid.next_batch().await.unwrap().unwrap();
        let outer_token = barrier.delivery_token().unwrap().clone();
        let outer_checkpoint = barrier.subscriber_checkpoint().unwrap().clone();
        assert_eq!(historical_polls.load(Ordering::SeqCst), historical_before);
        assert_eq!(live_polls.load(Ordering::SeqCst), live_before);

        let position = SubscriberResumePosition::new(
            1,
            block_ref(101),
            vec![block_ref(100), block_ref(101)],
            Some(outer_token.clone()),
            Some(outer_checkpoint),
        );
        drop(hybrid);

        let restored_historical_polls = Arc::new(AtomicUsize::new(0));
        let restored_live_polls = Arc::new(AtomicUsize::new(0));
        let historical_restores = Arc::new(Mutex::new(Vec::new()));
        let live_restores = Arc::new(Mutex::new(Vec::new()));
        let history = DurableLifecycleSubscriber {
            historical: true,
            steps: VecDeque::new(),
            polls: Arc::clone(&restored_historical_polls),
            restores: Arc::clone(&historical_restores),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
        };
        let live = DurableLifecycleSubscriber {
            historical: false,
            steps: VecDeque::new(),
            polls: Arc::clone(&restored_live_polls),
            restores: Arc::clone(&live_restores),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
        };
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        restored
            .prepare_restore_base_lifecycle(&position, &[])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();

        let live_resume = live_restores.lock().unwrap()[0].clone();
        assert_eq!(live_resume.coverage_head, block_ref(101));
        assert_eq!(live_resume.canonical_history, vec![block_ref(101)]);
        assert!(live_resume.delivery_token.is_none());
        assert_eq!(
            live_resume.subscriber_checkpoint.unwrap().as_bytes(),
            b"durable-empty-live-cursor"
        );
        assert_eq!(historical_restores.lock().unwrap().len(), 1);

        // A crash after the runtime persisted the barrier but before its ACK
        // is reconciled idempotently from the committed outer token.
        restored.acknowledge_delivery(outer_token).await.unwrap();
        assert!(restored.next_batch().await.unwrap().is_none());
        assert_eq!(restored_historical_polls.load(Ordering::SeqCst), 0);
        assert_eq!(restored_live_polls.load(Ordering::SeqCst), 0);
        assert_eq!(restored.phase(), HybridPhase::Live);
    })
    .await
    .expect("durable empty-lifecycle restore regression timed out");
}

#[tokio::test]
async fn clear_barrier_lost_outer_ack_is_restorable_with_an_ephemeral_live_child() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"ephemeral-empty-history-100"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, tokenless_batch(&[101]))],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut first = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        first
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = first.next_batch().await.unwrap().unwrap();
            first
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        first.register_interests(&[]).await.unwrap();
        let barrier = first.next_batch().await.unwrap().unwrap();
        let token = barrier.delivery_token().unwrap().clone();
        let position = SubscriberResumePosition::new(
            1,
            block_ref(101),
            vec![block_ref(100), block_ref(101)],
            Some(token.clone()),
            Some(barrier.subscriber_checkpoint().unwrap().clone()),
        );
        drop(first);

        let history =
            RestoreProbeSubscriber::new(true, Some(1), [Ok(())], Arc::new(Mutex::new(Vec::new())));
        let live_polls = Arc::new(AtomicUsize::new(0));
        let live = ScriptedSubscriber::new(
            "live",
            [],
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
        )
        .with_poll_counter(Arc::clone(&live_polls));
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        restored
            .prepare_restore_base_lifecycle(&position, &[])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();

        restored.acknowledge_delivery(token).await.unwrap();
        assert!(restored.next_batch().await.unwrap().is_none());
        assert_eq!(live_polls.load(Ordering::SeqCst), 0);
    })
    .await
    .expect("ephemeral empty-barrier lost-ACK restore timed out");
}

#[tokio::test]
async fn all_empty_owner_revision_emits_a_durable_barrier_then_restores_idle() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let owner = HandlerId::new("durable-empty-owner");
        let history = DurableLifecycleSubscriber {
            historical: true,
            steps: VecDeque::from([Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"empty-owner-history-100"),
            )]),
            polls: Arc::new(AtomicUsize::new(0)),
            restores: Arc::new(Mutex::new(Vec::new())),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
        };
        let live = DurableLifecycleSubscriber {
            historical: false,
            steps: VecDeque::from([Step::Batch(
                Duration::ZERO,
                batch(&[101], b"empty-owner-live-101"),
            )]),
            polls: Arc::new(AtomicUsize::new(0)),
            restores: Arc::new(Mutex::new(Vec::new())),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
        };
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .add_interest_owner(owner.clone(), &[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }

        hybrid
            .replace_interest_owners(vec![(owner.clone(), Vec::new())])
            .await
            .unwrap();
        let barrier = hybrid.next_batch().await.unwrap().unwrap();
        assert!(barrier.records().is_empty());
        assert!(matches!(
            barrier.chain_controls(),
            [ChainControl::Barrier { block: Some(block), .. }] if block.number == 101
        ));
        let outer_token = barrier.delivery_token().unwrap().clone();
        let position = SubscriberResumePosition::new(
            1,
            block_ref(101),
            vec![block_ref(100), block_ref(101)],
            Some(outer_token.clone()),
            Some(barrier.subscriber_checkpoint().unwrap().clone()),
        );
        drop(hybrid);

        let historical_polls = Arc::new(AtomicUsize::new(0));
        let live_polls = Arc::new(AtomicUsize::new(0));
        let history = DurableLifecycleSubscriber {
            historical: true,
            steps: VecDeque::new(),
            polls: Arc::clone(&historical_polls),
            restores: Arc::new(Mutex::new(Vec::new())),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
        };
        let live = DurableLifecycleSubscriber {
            historical: false,
            steps: VecDeque::new(),
            polls: Arc::clone(&live_polls),
            restores: Arc::new(Mutex::new(Vec::new())),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
        };
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        restored
            .prepare_restore_lifecycle(&position, &[], vec![(owner, Vec::new())])
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();
        restored.acknowledge_delivery(outer_token).await.unwrap();

        assert!(restored.next_batch().await.unwrap().is_none());
        assert_eq!(historical_polls.load(Ordering::SeqCst), 0);
        assert_eq!(live_polls.load(Ordering::SeqCst), 0);
        assert_eq!(restored.phase(), HybridPhase::Live);
    })
    .await
    .expect("durable all-empty owner restore regression timed out");
}

#[tokio::test]
async fn records_with_missing_or_wrong_chain_id_are_rejected_before_buffering() {
    tokio::time::timeout(Duration::from_secs(2), async {
        for chain_id in [None, Some(2)] {
            let registrations = Arc::new(Mutex::new(Vec::new()));
            let history = ScriptedSubscriber::new(
                "history",
                [Step::Batch(
                    Duration::from_millis(20),
                    batch(&[104], b"chain-history"),
                )],
                Arc::new(Mutex::new(Vec::new())),
                Arc::clone(&registrations),
            );
            let mut wrong = record(105);
            wrong.context.chain_id = chain_id;
            let live = ScriptedSubscriber::new(
                "live",
                [Step::Batch(
                    Duration::ZERO,
                    ReactiveInputBatch::new(vec![wrong])
                        .with_delivery_token(SubscriberDeliveryToken::new(b"wrong-chain".to_vec())),
                )],
                Arc::new(Mutex::new(Vec::new())),
                registrations,
            );
            let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
            hybrid
                .register_interests(&[portable_log_interest()])
                .await
                .unwrap();
            let error = hybrid.next_batch().await.expect_err("wrong-chain record");
            assert!(error.to_string().contains("expected 1"));
            assert_eq!(hybrid.buffered_live_batches(), 0);
        }
    })
    .await
    .expect("chain validation regression timed out");
}

#[test]
fn hybrid_capabilities_bridge_an_ephemeral_live_source_with_durable_history() {
    let history = CapabilitySubscriber(SubscriberCapabilities::new([
        SubscriberCapability::HistoricalBackfill,
        SubscriberCapability::Logs,
        SubscriberCapability::BlockHeaders,
        SubscriberCapability::OwnerScopedDelivery,
        SubscriberCapability::DynamicInterests,
        SubscriberCapability::ExplicitReorgs,
        SubscriberCapability::Barriers,
        SubscriberCapability::DurableReplay,
        SubscriberCapability::FinalityUpdates,
        SubscriberCapability::PendingTransactionHashes,
        SubscriberCapability::FullBlocks,
        SubscriberCapability::PendingTransactions,
    ]));
    let live = CapabilitySubscriber(SubscriberCapabilities::new([
        SubscriberCapability::Live,
        SubscriberCapability::Logs,
        SubscriberCapability::BlockHeaders,
        SubscriberCapability::OwnerScopedDelivery,
        SubscriberCapability::DynamicInterests,
        SubscriberCapability::ExplicitReorgs,
        SubscriberCapability::Barriers,
        SubscriberCapability::FinalityUpdates,
        SubscriberCapability::PendingTransactionHashes,
        SubscriberCapability::FullBlocks,
        SubscriberCapability::PendingTransactions,
    ]));
    let hybrid =
        HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
    let capabilities = hybrid.capabilities();
    for capability in [
        SubscriberCapability::HistoricalBackfill,
        SubscriberCapability::Live,
        SubscriberCapability::Logs,
        SubscriberCapability::OwnerScopedDelivery,
        SubscriberCapability::DynamicInterests,
        SubscriberCapability::ExplicitReorgs,
        SubscriberCapability::FinalityUpdates,
        SubscriberCapability::PendingTransactionHashes,
        SubscriberCapability::Barriers,
    ] {
        assert!(capabilities.supports(capability), "missing {capability:?}");
    }
    assert!(capabilities.supports(SubscriberCapability::DurableReplay));
    assert!(
        !capabilities.supports(SubscriberCapability::BlockHeaders),
        "generic child capability intersection is not proof of an exact header-body commitment"
    );
    assert!(!capabilities.supports(SubscriberCapability::FullBlocks));
    assert!(!capabilities.supports(SubscriberCapability::PendingTransactions));
}

#[test]
fn hybrid_advertises_durable_replay_from_its_historical_recovery_authority() {
    let history = CapabilitySubscriber(SubscriberCapabilities::new([
        SubscriberCapability::HistoricalBackfill,
        SubscriberCapability::DurableReplay,
        SubscriberCapability::Barriers,
    ]));
    let live = CapabilitySubscriber(SubscriberCapabilities::new([
        SubscriberCapability::Live,
        SubscriberCapability::DurableReplay,
    ]));
    let hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    assert!(
        hybrid
            .capabilities()
            .supports(SubscriberCapability::DurableReplay)
    );
}

#[tokio::test]
async fn core_restore_prepares_ephemeral_live_then_recovers_from_durable_history() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let (first, position, metadata, interest, revision, historical_mutations) =
            revisioned_checkpoint_fixture().await;
        drop(first);
        assert_eq!(revision.load(Ordering::SeqCst), 1);

        let history_acks = Arc::new(Mutex::new(Vec::new()));
        let historical_restores = Arc::new(Mutex::new(Vec::new()));
        let history = RevisionedHistoricalSubscriber {
            steps: VecDeque::from([Step::Batch(
                Duration::from_millis(10),
                batch(&[105], b"history-recovery-105").with_subscriber_checkpoint(
                    SubscriberCheckpoint::new(1u64.to_be_bytes().to_vec()),
                ),
            )]),
            revision,
            mutations: Arc::clone(&historical_mutations),
            acknowledgements: Arc::clone(&history_acks),
            restores: Arc::clone(&historical_restores),
        };
        let live_registered = Arc::new(AtomicBool::new(false));
        let live_restore_calls = Arc::new(AtomicUsize::new(0));
        let live = RegistrationRequiredLiveSubscriber {
            steps: VecDeque::from([Step::Batch(Duration::ZERO, tokenless_batch(&[106]))]),
            registered: Arc::clone(&live_registered),
            mutations: Arc::new(AtomicUsize::new(0)),
            restore_calls: Arc::clone(&live_restore_calls),
        };
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        assert!(
            hybrid
                .capabilities()
                .supports(SubscriberCapability::DurableReplay)
        );
        hybrid
            .prepare_restore_lifecycle(&position, std::slice::from_ref(&interest), Vec::new())
            .await
            .expect("live-first restore preparation");
        assert!(live_registered.load(Ordering::SeqCst));
        assert_eq!(
            historical_mutations.load(Ordering::SeqCst),
            1,
            "restore preparation must not mutate historical authority"
        );

        let runtime = ReactiveRuntime::new(ReactiveConfig::default());
        let mut engine = ReactiveEngine::new(runtime, hybrid);
        engine
            .resume_from_durable_checkpoint(&metadata)
            .expect("core accepts historical-backed Hybrid durability");
        assert_eq!(live_restore_calls.load(Ordering::SeqCst), 0);
        assert_eq!(historical_restores.lock().unwrap().len(), 1);

        let recovery = engine.subscriber_mut().next_batch().await.unwrap().unwrap();
        assert!(
            recovery.records().is_empty(),
            "checkpointed live input is witness-validated, not re-applied"
        );
        engine
            .subscriber_mut()
            .acknowledge_delivery(recovery.delivery_token().unwrap().clone())
            .await
            .unwrap();
        assert_eq!(
            history_acks.lock().unwrap().len(),
            2,
            "restore retries the persisted delivery ACK before acknowledging the overlap page"
        );

        let live = engine.subscriber_mut().next_batch().await.unwrap().unwrap();
        assert_eq!(
            live.records()[0].context.block.as_ref().unwrap().number,
            106
        );
    })
    .await
    .expect("core Hybrid recovery timed out");
}

#[tokio::test]
async fn nonempty_restore_requires_matching_live_lifecycle_preparation() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let (first, position, _metadata, interest, revision, historical_mutations) =
            revisioned_checkpoint_fixture().await;
        drop(first);

        let historical_restores = Arc::new(Mutex::new(Vec::new()));
        let history = RevisionedHistoricalSubscriber {
            steps: VecDeque::new(),
            revision,
            mutations: Arc::clone(&historical_mutations),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
            restores: Arc::clone(&historical_restores),
        };
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let live = RegistrationRequiredLiveSubscriber {
            steps: VecDeque::new(),
            registered: Arc::new(AtomicBool::new(false)),
            mutations: Arc::clone(&live_mutations),
            restore_calls: Arc::new(AtomicUsize::new(0)),
        };
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();

        let error = restored
            .restore_position(&position)
            .expect_err("non-empty lifecycle must be prepared");
        assert!(error.to_string().contains("prepare_restore_lifecycle"));
        assert!(historical_restores.lock().unwrap().is_empty());
        assert_eq!(live_mutations.load(Ordering::SeqCst), 0);

        let wrong_interest = ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(Address::repeat_byte(0xbb)),
            local_matcher: None,
            route_key: None,
        });
        let error = restored
            .prepare_restore_lifecycle(&position, &[wrong_interest], Vec::new())
            .await
            .expect_err("checkpoint lifecycle mismatch");
        assert!(error.to_string().contains("lifecycle intent"));
        assert_eq!(live_mutations.load(Ordering::SeqCst), 0);
        assert_eq!(historical_mutations.load(Ordering::SeqCst), 1);

        restored
            .prepare_restore_lifecycle(&position, &[interest], Vec::new())
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();
        assert_eq!(live_mutations.load(Ordering::SeqCst), 1);
        assert_eq!(historical_restores.lock().unwrap().len(), 1);
    })
    .await
    .expect("lifecycle-preparation regression timed out");
}

#[tokio::test]
async fn lifecycle_crash_window_rejects_an_older_hybrid_checkpoint_without_mutating_history() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let (first, position, metadata, interest, revision, historical_mutations) =
            revisioned_checkpoint_fixture().await;
        let newer_interest = ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(Address::repeat_byte(0xcc)),
            local_matcher: None,
            route_key: None,
        });
        drop(first);

        let mut history = RevisionedHistoricalSubscriber {
            steps: VecDeque::new(),
            revision,
            mutations: Arc::clone(&historical_mutations),
            acknowledgements: Arc::new(Mutex::new(Vec::new())),
            restores: Arc::new(Mutex::new(Vec::new())),
        };
        history
            .register_interests(&[newer_interest])
            .await
            .expect("remote lifecycle revision commits outside the crashed coordinator");
        assert_eq!(history.revision.load(Ordering::SeqCst), 2);
        let live = RegistrationRequiredLiveSubscriber {
            steps: VecDeque::new(),
            registered: Arc::new(AtomicBool::new(false)),
            mutations: Arc::new(AtomicUsize::new(0)),
            restore_calls: Arc::new(AtomicUsize::new(0)),
        };
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .prepare_restore_lifecycle(&position, std::slice::from_ref(&interest), Vec::new())
            .await
            .expect("rebuild the old runtime's live subscriptions");
        assert_eq!(
            historical_mutations.load(Ordering::SeqCst),
            2,
            "preparation cannot roll historical desired state backward"
        );

        let mut engine =
            ReactiveEngine::new(ReactiveRuntime::new(ReactiveConfig::default()), hybrid);
        let error = engine
            .resume_from_durable_checkpoint(&metadata)
            .expect_err("newer remote desired state must reject the older cache checkpoint");
        assert!(error.to_string().contains("desired-state revision"));
        assert_eq!(historical_mutations.load(Ordering::SeqCst), 2);
        assert!(
            engine
                .subscriber_mut()
                .next_batch()
                .await
                .expect_err("partial restore remains fenced")
                .to_string()
                .contains("restore reconciliation is pending")
        );
    })
    .await
    .expect("lifecycle crash-window regression timed out");
}

#[tokio::test]
async fn cancellation_after_historical_owner_commit_reconciles_both_sources_before_delivery() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let historical_owner = Arc::new(AtomicBool::new(false));
        let live_owner = Arc::new(AtomicBool::new(false));
        let historical_removals = Arc::new(AtomicUsize::new(0));
        let live_removals = Arc::new(AtomicUsize::new(0));
        let history = LifecycleStateSubscriber {
            historical: true,
            steps: VecDeque::new(),
            owner_installed: Arc::clone(&historical_owner),
            removals: Arc::clone(&historical_removals),
            block_first_add_after_commit: true,
        };
        let live = LifecycleStateSubscriber {
            historical: false,
            steps: VecDeque::new(),
            owner_installed: Arc::clone(&live_owner),
            removals: Arc::clone(&live_removals),
            block_first_add_after_commit: false,
        };
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        let owner = HandlerId::new("uncertain-owner");
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                hybrid.add_interest_owner(owner.clone(), &[]),
            )
            .await
            .is_err()
        );
        assert!(historical_owner.load(Ordering::SeqCst));
        assert!(live_owner.load(Ordering::SeqCst));
        assert!(
            hybrid
                .next_batch()
                .await
                .expect_err("uncertain lifecycle blocks delivery")
                .to_string()
                .contains("lifecycle reconciliation is pending")
        );

        hybrid
            .add_interest_owner(owner, &[])
            .await
            .expect("exact retry rolls both back, then commits both");
        assert!(historical_owner.load(Ordering::SeqCst));
        assert!(live_owner.load(Ordering::SeqCst));
        assert_eq!(historical_removals.load(Ordering::SeqCst), 1);
        assert_eq!(live_removals.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("uncertain lifecycle regression timed out");
}

#[tokio::test]
async fn cancellation_during_canonical_owner_activation_reconciles_before_retry() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let historical_owner = Arc::new(AtomicBool::new(false));
        let live_owner = Arc::new(AtomicBool::new(false));
        let historical_removals = Arc::new(AtomicUsize::new(0));
        let live_removals = Arc::new(AtomicUsize::new(0));
        let history = LifecycleStateSubscriber {
            historical: true,
            steps: VecDeque::from([Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"retained-history-100"),
            )]),
            owner_installed: Arc::clone(&historical_owner),
            removals: Arc::clone(&historical_removals),
            block_first_add_after_commit: true,
        };
        let live = LifecycleStateSubscriber {
            historical: false,
            steps: VecDeque::from([Step::Batch(Duration::ZERO, tokenless_batch(&[101]))]),
            owner_installed: Arc::clone(&live_owner),
            removals: Arc::clone(&live_removals),
            block_first_add_after_commit: false,
        };
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .upsert_interest_owners(vec![(
                HandlerId::new("seed-owner"),
                vec![portable_log_interest()],
            )])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = tokio::time::timeout(Duration::from_millis(250), hybrid.next_batch())
                .await
                .expect("canonical cancellation seed delivery")
                .unwrap()
                .unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        assert_eq!(hybrid.phase(), HybridPhase::Live);
        let owner = HandlerId::new("uncertain-canonical-owner");
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                hybrid.add_interest_owner_with_canonical_catchup(
                    owner.clone(),
                    &[portable_log_interest()],
                    block_ref(100),
                ),
            )
            .await
            .is_err()
        );
        assert!(historical_owner.load(Ordering::SeqCst));
        assert!(live_owner.load(Ordering::SeqCst));
        assert!(
            tokio::time::timeout(Duration::from_millis(250), hybrid.next_batch())
                .await
                .expect("pending lifecycle must reject delivery promptly")
                .expect_err("uncertain activation blocks delivery")
                .to_string()
                .contains("lifecycle reconciliation is pending")
        );

        tokio::time::timeout(
            Duration::from_millis(250),
            hybrid.add_interest_owner_with_canonical_catchup(
                owner,
                &[portable_log_interest()],
                block_ref(100),
            ),
        )
        .await
        .expect("retry must reconcile promptly")
        .expect("exact retry rolls both children back before reactivation");
        assert!(historical_owner.load(Ordering::SeqCst));
        assert!(live_owner.load(Ordering::SeqCst));
        assert_eq!(historical_removals.load(Ordering::SeqCst), 1);
        assert_eq!(live_removals.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("uncertain canonical lifecycle regression timed out");
}

#[tokio::test]
async fn alloy_removed_log_without_parent_rewinds_to_exact_retained_predecessor() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::from_millis(10), batch(&[100], b"history-100"))],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let removed = ReactiveInputBatch::new(vec![removed_record_for_block(
            block_ref(101),
            B256::repeat_byte(102),
        )])
        .with_delivery_token(SubscriberDeliveryToken::new(b"removed-101".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                Step::Batch(Duration::from_millis(10), removed),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        let removed = hybrid.next_batch().await.unwrap().unwrap();
        assert!(matches!(
            &removed.records()[0].context.chain_status,
            ChainStatus::Reorged { dropped_from } if dropped_from.number == 101 && dropped_from.parent_hash.is_none()
        ));
        hybrid
            .acknowledge_delivery(removed.delivery_token().unwrap().clone())
            .await
            .unwrap();
        assert_eq!(hybrid.phase(), HybridPhase::Live);
    })
    .await
    .expect("parentless removed-log regression timed out");
}

#[tokio::test]
async fn sparse_parentless_drop_rewinds_to_the_last_exact_retained_block() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100, 105], b"history-sparse"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let removed = ReactiveInputBatch::new(vec![removed_record_for_block(
            block_ref(105),
            B256::repeat_byte(106),
        )])
        .with_delivery_token(SubscriberDeliveryToken::new(b"removed-105".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[106], b"live-106")),
                Step::Batch(Duration::from_millis(10), removed),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..3 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        assert_eq!(hybrid.phase(), HybridPhase::Live);
    })
    .await
    .expect("sparse parentless-drop regression timed out");
}

#[tokio::test]
async fn parentless_removed_log_and_implicit_replacement_commit_in_one_batch() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"history-100"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let replacement = ReactiveInputBatch::new(vec![
            removed_record_for_block(block_ref(101), B256::repeat_byte(102)),
            replacement_record(101, 0xf1, 0xf2),
        ])
        .with_delivery_token(SubscriberDeliveryToken::new(
            b"implicit-replacement".to_vec(),
        ));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                Step::Batch(Duration::from_millis(10), replacement),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        let replacement = hybrid.next_batch().await.unwrap().unwrap();
        assert_eq!(replacement.records().len(), 2);
        hybrid
            .acknowledge_delivery(replacement.delivery_token().unwrap().clone())
            .await
            .unwrap();
        assert_eq!(hybrid.phase(), HybridPhase::Live);
    })
    .await
    .expect("implicit replacement regression timed out");
}

#[tokio::test]
async fn replacement_that_contradicts_a_retained_parent_poisons_instead_of_recovering() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"known-anchor-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let old_tip = block_ref(101);
        let mut replacement = block_ref(101);
        replacement.hash = B256::repeat_byte(0xf1);
        replacement.parent_hash = Some(B256::repeat_byte(0xfe));
        let malformed = ReactiveInputBatch::new(vec![
            removed_record_for_block(old_tip, B256::repeat_byte(102)),
            record_for_block(replacement, B256::repeat_byte(0xf2)),
        ])
        .with_delivery_token(SubscriberDeliveryToken::new(
            b"known-anchor-conflict".to_vec(),
        ));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"known-anchor-live")),
                Step::Batch(Duration::from_millis(10), malformed),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        assert_eq!(hybrid.phase(), HybridPhase::Live);

        let error = hybrid
            .next_batch()
            .await
            .expect_err("known retained-parent contradiction must fail closed");

        assert!(
            error
                .to_string()
                .contains("canonical sequence validation failed")
        );
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
        assert!(hybrid.poison_reason().is_some());
    })
    .await
    .expect("known-anchor poison regression timed out");
}

#[tokio::test]
async fn same_batch_headers_with_one_identity_and_different_visible_bodies_fail_closed() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let commitment = SubscriberPayloadCommitment::new(B256::repeat_byte(0x71));
    let history_batch = ReactiveInputBatch::new(vec![
        header_record_with_gas_limit(100, 10_000_000),
        header_record_with_gas_limit(100, 20_000_000),
    ])
    .with_payload_commitment(commitment)
    .with_delivery_token(SubscriberDeliveryToken::new(b"header-conflict".to_vec()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::ZERO, history_batch)],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), registrations);
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();
    let error = hybrid
        .next_batch()
        .await
        .expect_err("header bodies conflict");
    assert!(
        error.to_string().contains("conflicting payload"),
        "unexpected error: {error}"
    );
    assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
}

#[tokio::test]
async fn cross_source_headers_with_one_identity_and_different_visible_bodies_fail_closed() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let commitment = SubscriberPayloadCommitment::new(B256::repeat_byte(0x72));
        let history_batch =
            ReactiveInputBatch::new(vec![header_record_with_gas_limit(100, 10_000_000)])
                .with_payload_commitment(commitment)
                .with_delivery_token(SubscriberDeliveryToken::new(b"history-header".to_vec()));
        let live_batch =
            ReactiveInputBatch::new(vec![header_record_with_gas_limit(100, 20_000_000)])
                .with_payload_commitment(commitment)
                .with_delivery_token(SubscriberDeliveryToken::new(b"live-header".to_vec()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(Duration::from_millis(10), history_batch)],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, live_batch)],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        let historical = hybrid.next_batch().await.unwrap().unwrap();
        hybrid
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let error = hybrid
            .next_batch()
            .await
            .expect_err("header bodies conflict");
        assert!(error.to_string().contains("conflicting payload"));
        assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
    })
    .await
    .expect("cross-source header regression timed out");
}

#[tokio::test]
async fn serialized_header_body_is_charged_against_the_real_ingress_bound() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history_batch =
        ReactiveInputBatch::new(vec![header_record_with_gas_limit(100, 30_000_000)])
            .with_payload_commitment(SubscriberPayloadCommitment::new(B256::repeat_byte(0x73)))
            .with_delivery_token(SubscriberDeliveryToken::new(b"large-header".to_vec()));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::ZERO, history_batch)],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), registrations);
    let mut config = HybridConfig::default();
    config.max_buffered_live_bytes = 700;
    let mut hybrid = HybridSubscriber::new(history, live, config).unwrap();
    hybrid
        .register_interests(&[portable_log_interest()])
        .await
        .unwrap();
    let error = hybrid
        .next_batch()
        .await
        .expect_err("header exceeds byte budget");
    assert!(
        error.to_string().contains("serialized block header")
            || error.to_string().contains("ingress byte bound")
    );
}

#[tokio::test]
async fn empty_base_topology_is_live_and_immediately_accepts_the_first_owner() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let history = LifecycleMethodProbeSubscriber {
        historical: true,
        calls: Arc::clone(&calls),
        steps: VecDeque::new(),
    };
    let live = LifecycleMethodProbeSubscriber {
        historical: false,
        calls,
        steps: VecDeque::new(),
    };
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid.register_interests(&[]).await.unwrap();
    assert_eq!(hybrid.phase(), HybridPhase::Live);
    hybrid
        .add_interest_owner(HandlerId::new("first-owner"), &[])
        .await
        .expect("empty base mode can transition to its first owner without a source batch");
    assert_eq!(hybrid.phase(), HybridPhase::Live);
}

#[tokio::test]
async fn empty_owner_topology_is_live_and_immediately_reconfigurable() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let history = LifecycleMethodProbeSubscriber {
        historical: true,
        calls: Arc::clone(&calls),
        steps: VecDeque::new(),
    };
    let live = LifecycleMethodProbeSubscriber {
        historical: false,
        calls,
        steps: VecDeque::new(),
    };
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    hybrid.replace_interest_owners(Vec::new()).await.unwrap();
    assert_eq!(hybrid.phase(), HybridPhase::Live);
    hybrid
        .add_interest_owner(HandlerId::new("first-owner"), &[])
        .await
        .expect("empty owner mode can accept its first owner without a source batch");
    assert_eq!(hybrid.phase(), HybridPhase::Live);
}

#[tokio::test]
async fn base_restore_preparation_compiles_and_runs_with_non_owner_children() {
    let (first, position, _metadata, interest, _revision, _mutations) =
        revisioned_checkpoint_fixture().await;
    drop(first);
    let history = CapabilitySubscriber(SubscriberCapabilities::new([
        SubscriberCapability::HistoricalBackfill,
        SubscriberCapability::DurableReplay,
        SubscriberCapability::Barriers,
        SubscriberCapability::Logs,
    ]));
    let live = CapabilitySubscriber(SubscriberCapabilities::new([
        SubscriberCapability::Live,
        SubscriberCapability::Logs,
    ]));
    let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    restored
        .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
        .await
        .expect("base preparation requires only EventSubscriber");
    restored.restore_position(&position).unwrap();
    assert_eq!(restored.phase(), HybridPhase::Recovering);
}

#[tokio::test]
async fn ephemeral_live_restore_reuses_raw_token_one_in_a_fresh_outer_epoch() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let interest = portable_log_interest();
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"history-100"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, batch(&[101], &[1]))],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut first = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        first
            .register_interests(std::slice::from_ref(&interest))
            .await
            .unwrap();
        let historical = first.next_batch().await.unwrap().unwrap();
        first
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let live = first.next_batch().await.unwrap().unwrap();
        let old_outer_token = live.delivery_token().unwrap().clone();
        let checkpoint = live.subscriber_checkpoint().unwrap().clone();
        first
            .acknowledge_delivery(old_outer_token.clone())
            .await
            .unwrap();
        drop(first);

        let position = SubscriberResumePosition::new(
            1,
            block_ref(101),
            vec![block_ref(100), block_ref(101)],
            Some(old_outer_token.clone()),
            Some(checkpoint),
        );
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[101], b"recovery-101"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(Duration::ZERO, batch(&[102], &[1]))],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        restored
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .unwrap();
        restored.restore_position(&position).unwrap();
        let historical = restored.next_batch().await.unwrap().unwrap();
        restored
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let reused = restored.next_batch().await.unwrap().unwrap();
        assert_eq!(
            reused.records().len(),
            1,
            "raw token reuse is not ACK-only replay"
        );
        assert_ne!(reused.delivery_token(), Some(&old_outer_token));
        assert_eq!(
            reused.records()[0].context.block.as_ref().unwrap().number,
            102
        );
    })
    .await
    .expect("ephemeral token namespace regression timed out");
}

#[tokio::test]
async fn malformed_reorg_shapes_are_rejected_before_an_output_becomes_pending() {
    let mut replacement = block_ref(101);
    replacement.hash = B256::repeat_byte(0xf1);
    replacement.parent_hash = Some(B256::repeat_byte(0xf2));
    let cases = [
        ChainControl::Reorg {
            common_ancestor: block_ref(101),
            old_tip: block_ref(101),
            new_tip: block_ref(102),
        },
        ChainControl::Reorg {
            common_ancestor: block_ref(100),
            old_tip: block_ref(102),
            new_tip: block_ref(103),
        },
        ChainControl::Reorg {
            common_ancestor: block_ref(100),
            old_tip: block_ref(101),
            new_tip: replacement,
        },
    ];
    for (index, control) in cases.into_iter().enumerate() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let registrations = Arc::new(Mutex::new(Vec::new()));
            let history = ScriptedSubscriber::new(
                "history",
                [Step::Batch(
                    Duration::from_millis(10),
                    batch(&[100], b"history-100"),
                )],
                Arc::new(Mutex::new(Vec::new())),
                Arc::clone(&registrations),
            );
            let malformed = ReactiveInputBatch::new(Vec::new())
                .with_chain_id(1)
                .with_chain_controls([control])
                .with_delivery_token(SubscriberDeliveryToken::new(vec![0xe0, index as u8]));
            let live = ScriptedSubscriber::new(
                "live",
                [
                    Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                    Step::Batch(Duration::from_millis(10), malformed),
                ],
                Arc::new(Mutex::new(Vec::new())),
                registrations,
            );
            let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
            hybrid
                .register_interests(&[portable_log_interest()])
                .await
                .unwrap();
            for _ in 0..2 {
                let delivery = hybrid.next_batch().await.unwrap().unwrap();
                hybrid
                    .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                    .await
                    .unwrap();
            }
            let error = hybrid.next_batch().await.expect_err("malformed reorg");
            assert!(
                error.to_string().contains("invalid reorg")
                    || error.to_string().contains("invalid chain control"),
                "case {index} returned an unexpected error: {error}"
            );
            assert_eq!(hybrid.phase(), HybridPhase::Poisoned);
            assert!(
                hybrid
                    .next_batch()
                    .await
                    .expect_err("no pending malformed output")
                    .to_string()
                    .contains("poisoned")
            );
        })
        .await
        .expect("malformed reorg regression timed out");
    }
}

#[tokio::test]
async fn compatible_stale_progress_is_suppressed_without_creating_an_unacknowledgeable_batch() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"history-100"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let stale = ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::CanonicalProgress(block_ref(100))])
            .with_delivery_token(SubscriberDeliveryToken::new(b"stale-progress".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                Step::Batch(Duration::from_millis(10), stale),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        let stale = hybrid.next_batch().await.unwrap().unwrap();
        assert!(stale.records().is_empty());
        assert!(stale.chain_controls().is_empty());
        hybrid
            .acknowledge_delivery(stale.delivery_token().unwrap().clone())
            .await
            .unwrap();
    })
    .await
    .expect("stale progress regression timed out");
}

#[tokio::test]
async fn same_head_progress_can_enrich_missing_metadata() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"history-100"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live_record = record_with_block_metadata(101, block_ref(101).parent_hash, None);
        let live_seed = ReactiveInputBatch::new(vec![live_record])
            .with_chain_id(1)
            .with_delivery_token(SubscriberDeliveryToken::new(b"live-101".to_vec()));
        let enrichment = ReactiveInputBatch::new(Vec::new())
            .with_chain_id(1)
            .with_chain_controls([ChainControl::CanonicalProgress(block_ref(101))])
            .with_delivery_token(SubscriberDeliveryToken::new(b"enrich-101".to_vec()));
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, live_seed),
                Step::Batch(Duration::from_millis(10), enrichment),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        let enrichment = hybrid.next_batch().await.unwrap().unwrap();
        assert!(matches!(
            enrichment.chain_controls(),
            [ChainControl::CanonicalProgress(block)] if block.timestamp == block_ref(101).timestamp
        ));
    })
    .await
    .expect("progress enrichment regression timed out");
}

#[tokio::test]
async fn destructive_topology_compensation_backfills_the_lost_live_queue_before_live() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let historical_topology = Arc::new(Mutex::new(Vec::new()));
        let live_topology = Arc::new(Mutex::new(Vec::new()));
        let lifecycle_calls = Arc::new(Mutex::new(Vec::new()));
        let history = DestructiveTopologySubscriber {
            historical: true,
            steps: VecDeque::from([Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"history-100"),
            )]),
            topology: Arc::clone(&historical_topology),
            lifecycle_calls: Arc::clone(&lifecycle_calls),
            global_calls: 0,
            exact_calls: 0,
        };
        let live = DestructiveTopologySubscriber {
            historical: false,
            steps: VecDeque::from([
                Step::Batch(Duration::ZERO, batch(&[101], b"live-101")),
                // This queued event is deliberately destroyed by exact
                // replacement and must be recovered from history.
                Step::Batch(Duration::from_millis(50), batch(&[102], b"lost-live-102")),
            ]),
            topology: Arc::clone(&live_topology),
            lifecycle_calls: Arc::clone(&lifecycle_calls),
            global_calls: 0,
            exact_calls: 0,
        };
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        let old_owner = HandlerId::new("old-owner");
        let new_owner = HandlerId::new("new-owner");
        hybrid
            .upsert_interest_owners(vec![(old_owner.clone(), vec![portable_log_interest()])])
            .await
            .unwrap();
        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }
        assert_eq!(hybrid.phase(), HybridPhase::Live);

        let error = hybrid
            .replace_interest_owners_with_global_backfill(
                vec![(new_owner, vec![portable_log_interest()])],
                SubscriberBackfill::after_canonical_block(block_ref(101)).unwrap(),
            )
            .await
            .expect_err("uncertain historical commit requires compensation recovery");
        assert!(
            error.to_string().contains("gap certification")
                || error.to_string().contains("uncertain historical commit")
        );
        assert_eq!(
            *historical_topology.lock().unwrap(),
            vec![old_owner.clone()]
        );
        assert_eq!(*live_topology.lock().unwrap(), vec![old_owner]);
        assert_eq!(
            lifecycle_calls.lock().unwrap().as_slice(),
            &[(true, 102), (true, 102)],
            "requested replacement and compensating restore both use exact C+1 history"
        );

        let recovered = hybrid.next_batch().await.unwrap().unwrap();
        assert_eq!(
            recovered.records()[0]
                .context
                .block
                .as_ref()
                .unwrap()
                .number,
            102,
            "historical recovery must close the queue-loss gap first"
        );
        hybrid
            .acknowledge_delivery(recovered.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let live = hybrid.next_batch().await.unwrap().unwrap();
        assert_eq!(
            live.records()[0].context.block.as_ref().unwrap().number,
            103
        );
    })
    .await
    .expect("destructive compensation regression timed out");
}

#[tokio::test]
async fn many_owner_sparse_routing_stays_within_the_durable_budget() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let routed_owner = HandlerId::new("owner-000");
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                scoped_batch(&[100], routed_owner.clone(), b"sparse-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::ZERO,
                scoped_batch(&[101], routed_owner.clone(), b"sparse-live"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
        let owners = (0..128)
            .map(|index| {
                (
                    HandlerId::new(format!("owner-{index:03}")),
                    vec![portable_log_interest()],
                )
            })
            .collect();
        hybrid.upsert_interest_owners(owners).await.unwrap();

        let delivery = hybrid.next_batch().await.unwrap().unwrap();
        assert_eq!(
            delivery.record_audience(0),
            Some(&DeliveryAudience::Owners(vec![routed_owner]))
        );
        assert!(delivery.subscriber_checkpoint().is_some());
    })
    .await
    .expect("many-owner sparse-routing regression timed out");
}

#[tokio::test]
async fn oversized_handler_id_is_rejected_before_either_child_is_mutated() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&registrations),
    );
    let mut hybrid = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    let owner = HandlerId::new("x".repeat(HYBRID_MAX_HANDLER_ID_BYTES + 1));
    let error = hybrid
        .upsert_interest_owners(vec![(owner, vec![portable_log_interest()])])
        .await
        .expect_err("oversized durable handler id");
    assert!(error.to_string().contains("handler id"));
    assert!(registrations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn restore_rejects_a_recent_owner_window_over_local_config_before_live_mutation() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let owner = HandlerId::new("restore-budget-owner");
        let initial_calls = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(20),
                scoped_batch(&[100], owner.clone(), b"budget-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&initial_calls),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [Step::Batch(
                Duration::ZERO,
                scoped_batch(&[101], owner.clone(), b"budget-live"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            initial_calls,
        );
        let mut source_config = HybridConfig::default();
        source_config.max_recent_owner_entries = 8;
        let mut source = HybridSubscriber::new(history, live, source_config).unwrap();
        source
            .upsert_interest_owners(vec![(owner.clone(), vec![portable_log_interest()])])
            .await
            .unwrap();
        let historical = source.next_batch().await.unwrap().unwrap();
        source
            .acknowledge_delivery(historical.delivery_token().unwrap().clone())
            .await
            .unwrap();
        let live = source.next_batch().await.unwrap().unwrap();
        let position = resume_position(
            101,
            vec![block_ref(100), block_ref(101)],
            live.delivery_token().unwrap().clone(),
            live.subscriber_checkpoint().unwrap().clone(),
        );

        let restore_calls = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&restore_calls),
        );
        let live = ScriptedSubscriber::new(
            "live",
            [],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&restore_calls),
        );
        let mut restore_config = HybridConfig::default();
        restore_config.max_recent_owner_entries = 0;
        let mut restored = HybridSubscriber::new(history, live, restore_config).unwrap();
        let error = restored
            .prepare_restore_lifecycle(&position, &[], vec![(owner, vec![portable_log_interest()])])
            .await
            .expect_err("checkpoint exceeds local recent-owner budget");
        assert!(error.to_string().contains("durable verification budgets"));
        assert!(restore_calls.lock().unwrap().is_empty());
    })
    .await
    .expect("restore budget regression timed out");
}

#[tokio::test]
async fn restore_preflights_empty_journal_owner_topology_before_live_mutation() {
    let owner = HandlerId::new("empty-journal-owner");
    let source_calls = Arc::new(Mutex::new(Vec::new()));
    let progress = ReactiveInputBatch::new(Vec::new())
        .with_chain_id(1)
        .with_chain_controls([ChainControl::Barrier {
            id: b"owner-topology-only".to_vec(),
            block: Some(block_ref(100)),
        }])
        .with_delivery_token(SubscriberDeliveryToken::new(
            b"owner-topology-only".to_vec(),
        ));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::ZERO, progress)],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&source_calls),
    );
    let live = ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), source_calls);
    let mut source = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    source
        .upsert_interest_owners(vec![(owner.clone(), vec![portable_log_interest()])])
        .await
        .unwrap();
    let delivery = source.next_batch().await.unwrap().unwrap();
    assert!(delivery.records().is_empty());
    let position = resume_position(
        100,
        vec![block_ref(100)],
        delivery.delivery_token().unwrap().clone(),
        delivery.subscriber_checkpoint().unwrap().clone(),
    );

    let restore_calls = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&restore_calls),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&restore_calls),
    );
    let mut config = HybridConfig::default();
    config.max_recent_owner_entries = 0;
    let mut restored = HybridSubscriber::new(history, live, config).unwrap();
    let error = restored
        .prepare_restore_lifecycle(&position, &[], vec![(owner, vec![portable_log_interest()])])
        .await
        .expect_err("one-record owner checkpoint cannot fit local budget");

    assert!(
        error
            .to_string()
            .contains("cannot retain one protected delivery witness"),
        "unexpected capacity error: {error}"
    );
    assert!(restore_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn restore_preparation_rejects_mismatched_outer_token_before_live_mutation() {
    let owner = HandlerId::new("outer-token-owner");
    let source_calls = Arc::new(Mutex::new(Vec::new()));
    let progress = ReactiveInputBatch::new(Vec::new())
        .with_chain_id(1)
        .with_chain_controls([ChainControl::Barrier {
            id: b"outer-token-progress".to_vec(),
            block: Some(block_ref(100)),
        }])
        .with_delivery_token(SubscriberDeliveryToken::new(
            b"outer-token-progress".to_vec(),
        ));
    let history = ScriptedSubscriber::new(
        "history",
        [Step::Batch(Duration::ZERO, progress)],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&source_calls),
    );
    let live = ScriptedSubscriber::new("live", [], Arc::new(Mutex::new(Vec::new())), source_calls);
    let mut source = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    source
        .upsert_interest_owners(vec![(owner.clone(), vec![portable_log_interest()])])
        .await
        .unwrap();
    let delivery = source.next_batch().await.unwrap().unwrap();
    let position = SubscriberResumePosition::new(
        1,
        block_ref(100),
        vec![block_ref(100)],
        Some(SubscriberDeliveryToken::new(b"not-a-hybrid-token".to_vec())),
        delivery.subscriber_checkpoint().cloned(),
    );

    let restore_calls = Arc::new(Mutex::new(Vec::new()));
    let history = ScriptedSubscriber::new(
        "history",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&restore_calls),
    );
    let live = ScriptedSubscriber::new(
        "live",
        [],
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&restore_calls),
    );
    let mut restored = HybridSubscriber::new(history, live, HybridConfig::default()).unwrap();
    let error = restored
        .prepare_restore_lifecycle(&position, &[], vec![(owner, vec![portable_log_interest()])])
        .await
        .expect_err("outer token mismatch must fail during pure preflight");

    assert!(
        error.to_string().contains("token"),
        "unexpected restore preflight error: {error}"
    );
    assert!(restore_calls.lock().unwrap().is_empty());
}

#[test]
#[allow(clippy::field_reassign_with_default)] // Public HybridConfig is non-exhaustive.
fn constructor_rejects_checkpoint_windows_beyond_v5_limits() {
    let mut recent = HybridConfig::default();
    recent.recent_input_capacity = HYBRID_MAX_RECENT_INPUTS + 1;
    let mut history_window = HybridConfig::default();
    history_window.canonical_history_capacity = HYBRID_MAX_CANONICAL_HISTORY + 1;
    let mut owner_entries = HybridConfig::default();
    owner_entries.max_recent_owner_entries = HYBRID_MAX_RECENT_OWNER_ENTRIES + 1;
    let mut delivery_token = HybridConfig::default();
    delivery_token.max_source_delivery_token_bytes = HYBRID_MAX_SOURCE_DELIVERY_TOKEN_BYTES + 1;
    let mut source_checkpoint = HybridConfig::default();
    source_checkpoint.max_source_checkpoint_bytes = HYBRID_MAX_SOURCE_CHECKPOINT_BYTES + 1;
    for config in [
        recent,
        history_window,
        owner_entries,
        delivery_token,
        source_checkpoint,
    ] {
        let history = CapabilitySubscriber(SubscriberCapabilities::new([
            SubscriberCapability::HistoricalBackfill,
            SubscriberCapability::DurableReplay,
            SubscriberCapability::Barriers,
        ]));
        let live = CapabilitySubscriber(SubscriberCapabilities::new([SubscriberCapability::Live]));
        let error = HybridSubscriber::new(history, live, config)
            .err()
            .expect("oversized durable config");
        assert!(error.to_string().contains("v5 durable checkpoint limit"));
    }
}

#[test]
#[allow(clippy::field_reassign_with_default)] // Public HybridConfig is non-exhaustive.
fn constructor_rejects_zero_opaque_source_cursor_budgets() {
    for checkpoint in [false, true] {
        let mut config = HybridConfig::default();
        if checkpoint {
            config.max_source_checkpoint_bytes = 0;
        } else {
            config.max_source_delivery_token_bytes = 0;
        }
        let history = CapabilitySubscriber(SubscriberCapabilities::new([
            SubscriberCapability::HistoricalBackfill,
            SubscriberCapability::DurableReplay,
            SubscriberCapability::Barriers,
        ]));
        let live = CapabilitySubscriber(SubscriberCapabilities::new([SubscriberCapability::Live]));

        let error = HybridSubscriber::new(history, live, config)
            .err()
            .expect("zero cursor budget");
        assert!(error.to_string().contains("must be non-zero"));
    }
}

#[test]
fn constructor_rejects_an_ephemeral_historical_source() {
    let history = CapabilitySubscriber(SubscriberCapabilities::new([
        SubscriberCapability::HistoricalBackfill,
        SubscriberCapability::Barriers,
    ]));
    let live = CapabilitySubscriber(SubscriberCapabilities::new([
        SubscriberCapability::Live,
        SubscriberCapability::Logs,
    ]));
    let error = HybridSubscriber::new(history, live, HybridConfig::default())
        .err()
        .expect("historical durable replay is required");
    assert!(error.to_string().contains("durable backfill"));
}

#[tokio::test]
async fn live_rollback_outside_retained_history_enters_recovery_without_poisoning() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let history = ScriptedSubscriber::new(
            "history",
            [Step::Batch(
                Duration::from_millis(10),
                batch(&[100], b"structured-recovery-history"),
            )],
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&registrations),
        );
        let replacement = BlockRef {
            number: 102,
            hash: B256::repeat_byte(0xe2),
            parent_hash: None,
            timestamp: Some(1_700_000_102),
        };
        let live = ScriptedSubscriber::new(
            "live",
            [
                Step::Batch(Duration::ZERO, batch(&[101], b"structured-live-101")),
                Step::Batch(
                    Duration::ZERO,
                    reorg_batch(
                        block_ref(50),
                        block_ref(101),
                        replacement,
                        record_for_block(replacement, B256::repeat_byte(0xe3)),
                        b"structured-deep-reorg",
                    ),
                ),
            ],
            Arc::new(Mutex::new(Vec::new())),
            registrations,
        );
        let mut hybrid =
            HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");
        hybrid
            .register_interests(&[portable_log_interest()])
            .await
            .expect("registration");

        for _ in 0..2 {
            let delivery = hybrid.next_batch().await.unwrap().unwrap();
            hybrid
                .acknowledge_delivery(delivery.delivery_token().unwrap().clone())
                .await
                .unwrap();
        }

        let error = hybrid
            .next_batch()
            .await
            .expect_err("deep rollback needs durable history");
        assert!(
            error.to_string().contains("durable historical recovery"),
            "unexpected structured recovery error: {error}"
        );
        assert_eq!(hybrid.phase(), HybridPhase::Recovering);
        assert!(hybrid.poison_reason().is_none());
    })
    .await
    .expect("structured recovery regression timed out");
}
