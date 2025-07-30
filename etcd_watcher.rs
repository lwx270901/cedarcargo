use cedar_policy::{PolicySet, Schema, Entities, Policy};
use etcd_client::{Client, EventType, Error as EtcdError, WatchOptions};
use rocket::futures::StreamExt;
use log::{debug, error, info, warn};
use rocket::serde::json::serde_json;
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use std::str::FromStr;
use std::fmt::Debug;

/// Prefixes for etcd keys
const POLICY_PREFIX: &str = "cedar/policy/";
const SCHEMA_PREFIX: &str = "cedar/schema/";
const DATA_PREFIX: &str = "cedar/data/";

/// Configuration for the EtcdWatcher
#[derive(Debug, Clone)]
pub struct EtcdWatcherConfig {
    /// Etcd endpoints
    pub endpoints: Vec<String>,
    /// Reconnect delay in seconds when connection is lost
    pub reconnect_delay_secs: u64,
}

impl Default for EtcdWatcherConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["localhost:2379".to_string()],
            reconnect_delay_secs: 5,
        }
    }
}

/// EtcdWatcher watches etcd for changes to Cedar policies, schema, and data
pub struct EtcdWatcher {
    client: Client,
    policy_set: Arc<RwLock<PolicySet>>,
    schema: Arc<RwLock<Schema>>,
    entities: Arc<RwLock<Entities>>,
    config: EtcdWatcherConfig,
}

impl EtcdWatcher {
    /// Create a new EtcdWatcher
    pub async fn new(
        policy_set: Arc<RwLock<PolicySet>>,
        schema: Arc<RwLock<Schema>>,
        entities: Arc<RwLock<Entities>>,
        config: EtcdWatcherConfig,
    ) -> Result<Self, EtcdError> {
        let client = Client::connect(&config.endpoints, None).await?;
        Ok(Self {
            client,
            policy_set,
            schema,
            entities,
            config,
        })
    }

    /// Start watching etcd for changes
    pub async fn start_watching(&self) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        
        // Watch for policy changes
        let policy_handle = self.watch_prefix::<PolicySet>(
            POLICY_PREFIX, 
            self.policy_set.clone(),
            Self::update_policy
        );
        handles.push(policy_handle);
        
        // Watch for schema changes
        let schema_handle = self.watch_prefix::<Schema>(
            SCHEMA_PREFIX,
            self.schema.clone(),
            Self::update_schema
        );
        handles.push(schema_handle);
        
        // Watch for data changes
        let data_handle = self.watch_prefix::<Entities>(
            DATA_PREFIX,
            self.entities.clone(),
            Self::update_data
        );
        handles.push(data_handle);
        
        handles
    }
    
    /// Watch a specific prefix in etcd and apply updates using the provided handler function
    fn watch_prefix<T>(&self, prefix: &'static str, target: Arc<RwLock<T>>, handler: fn(&str, &str) -> Result<T, String>) -> JoinHandle<()>
    where
        T: Send + Sync + 'static + Debug,
    {
        let endpoints = self.config.endpoints.clone();
        let reconnect_delay = self.config.reconnect_delay_secs;
        
        tokio::spawn(async move {
            loop {
                let result = Self::watch_and_handle(endpoints.clone(), prefix, target.clone(), handler).await;
                if let Err(e) = result {
                    error!("Error watching etcd for {}: {}", prefix, e);
                    info!("Reconnecting to etcd in {} seconds", reconnect_delay);
                    sleep(Duration::from_secs(reconnect_delay)).await;
                }
            }
        })
    }
    
    /// Watch etcd for changes and handle them using the provided handler
    async fn watch_and_handle<T>(
        endpoints: Vec<String>,
        prefix: &str,
        target: Arc<RwLock<T>>,
        handler: fn(&str, &str) -> Result<T, String>,
    ) -> Result<(), String>
    where
        T: Send + Sync + 'static + Debug,
    {
        let mut client = match Client::connect(&endpoints, None).await {
            Ok(client) => client,
            Err(e) => return Err(format!("Failed to connect to etcd: {}", e)),
        };

        // First, load all existing entries
        Self::load_existing_entries(&mut client, prefix, target.clone(), handler).await?;
        
        // Then start watching for changes
        let options = WatchOptions::new().with_prefix();
        let (_watcher, mut stream) = match client.watch(prefix, Some(options)).await {
            Ok((watcher, stream)) => (watcher, stream),
            Err(e) => return Err(format!("Failed to create watcher: {}", e)),
        };
        
        info!("Started watching etcd prefix: {}", prefix);
        
        while let Some(resp) = stream.next().await {
            match resp {
        Ok(resp) => {
            for event in resp.events() {
                if let Some(kv) = event.kv() {
                    let key = match kv.key_str() {
                        Ok(k) => k,
                        Err(_) => continue,
                    };

                    match event.event_type() {
                        EventType::Put => {
                            match kv.value_str() {
                                Ok(value) => {
                                    debug!("Received update for key: {}", key);

                                    // --- Log previous value:
                                    let prev_value = {
                                        let target_guard = target.read().unwrap();
                                        // Attempt to serialize the previous value for logging
                                        format!("{:?}", &*target_guard)
                                    };
                                    
                                    match handler(key, value) {
                                        Ok(updated) => {
                                            // --- Log updated value:
                                            let updated_value = format!("{:?}", &updated);

                                            info!(
                                                "Key {} updated\nPrevious: {}\nUpdated: {}",
                                                key,
                                                prev_value,
                                                updated_value
                                            );

                                            let mut target_lock = target.write().unwrap();
                                            *target_lock = updated;
                                            info!("Updated {} from etcd", key);
                                        }
                                        Err(e) => {
                                            error!("Failed to update {} from etcd: {}", key, e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to read value for key {}: {}", key, e);
                                }
                            }
                        }
                        EventType::Delete => {
                            warn!("Deletion detected for key: {}. This may require special handling.", key);
                            // Additional deletion handling logic can be added here if needed
                        }
                    }
                }
            }
        }
        Err(e) => {
            error!("Watch error: {}", e);
            return Err(format!("Watch error: {}", e));
        }
    }
        }
        
        Err("Watch stream ended unexpectedly".to_string())
    }
    
    /// Load existing entries from etcd
    async fn load_existing_entries<T>(
        client: &mut Client,
        prefix: &str,
        target: Arc<RwLock<T>>,
        handler: fn(&str, &str) -> Result<T, String>,
    ) -> Result<(), String>
    where
        T: Send + Sync + 'static,
    {
        let options = etcd_client::GetOptions::new().with_prefix();
        let response = match client.get(prefix, Some(options)).await {
            Ok(resp) => resp,
            Err(e) => return Err(format!("Failed to get existing entries: {}", e)),
        };
        
        for kv in response.kvs() {
            if let (Ok(key), Ok(value)) = (kv.key_str(), kv.value_str()) {
                debug!("Loading existing entry: {}", key);
                match handler(key, value) {
                    Ok(updated) => {
                        let mut target_lock = target.write().unwrap();
                        *target_lock = updated;
                        info!("Loaded {} from etcd", key);
                    }
                    Err(e) => {
                        error!("Failed to load {} from etcd: {}", key, e);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Update policy from etcd value
    fn update_policy(_key: &str, value: &str) -> Result<PolicySet, String> {
        // Parse the policy from the value using Cedar's Policy parser
        match Policy::from_str(value) {
            Ok(policy) => {
                // Create a PolicySet from a single policy
                let mut policy_set = PolicySet::new();
                policy_set.add(policy).map_err(|e| format!("Failed to add policy: {}", e))?;
                Ok(policy_set)
            },
            Err(e) => Err(format!("Failed to parse policy: {}", e)),
        }
    }
    
    /// Update schema from etcd value
    fn update_schema(_key: &str, value: &str) -> Result<Schema, String> {
        // Parse the schema from the value
        let json_value: serde_json::Value = match serde_json::from_str(value) {
            Ok(v) => v,
            Err(e) => return Err(format!("Failed to parse JSON: {}", e)),
        };
        
        match Schema::from_json_value(json_value) {
            Ok(schema) => Ok(schema),
            Err(e) => Err(format!("Failed to parse schema: {}", e)),
        }
    }
    
    /// Update data from etcd value
    fn update_data(_key: &str, value: &str) -> Result<Entities, String> {
        // Parse the entities from the value
        let json_value: serde_json::Value = match serde_json::from_str(value) {
            Ok(v) => v,
            Err(e) => return Err(format!("Failed to parse JSON: {}", e)),
        };
        
        // Get the current schema for entity validation
        // In a real implementation, you might want to pass the schema as a parameter
        let schema_option = None; // No schema validation for now
        
        // Parse entities with the schema
        match cedar_policy::Entities::from_json_value(json_value, schema_option) {
            Ok(entities) => Ok(entities),
            Err(e) => Err(format!("Failed to parse entities: {}", e)),
        }
    }
}