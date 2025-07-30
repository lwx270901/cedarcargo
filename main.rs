extern crate core;
extern crate rocket;

use std::borrow::Borrow;
use std::process::ExitCode;

use log::{error, info};
use rocket::catchers;
use rocket::http::ContentType;
use rocket_okapi::settings::UrlObject;
use rocket_okapi::{openapi_get_routes, rapidoc::*, swagger_ui::*};
use clap::Parser;
use rocket::serde::json::serde_json;

use crate::services::data::memory::MemoryDataStore;
use crate::services::data::DataStore;
use crate::services::policies::memory::MemoryPolicyStore;
use crate::services::policies::PolicyStore;
use crate::services::schema::memory::MemorySchemaStore;
use crate::services::schema::SchemaStore;


mod authn;
mod common;
mod config;
mod errors;
mod logger;
mod routes;
mod schemas;
mod services;
mod etcd_watcher;
use etcd_watcher::{EtcdWatcher, EtcdWatcherConfig};
use std::sync::{Arc, RwLock};

// Simplified Args struct without env attribute to fix compilation
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Etcd endpoints (comma separated)
    #[arg(long, default_value = "localhost:2379")]
    etcd_endpoints: String,
    
    /// Enable etcd watcher
    #[arg(long, default_value = "true")]
    enable_etcd_watcher: bool,
    
    /// Etcd reconnect delay in seconds
    #[arg(long, default_value = "5")]
    etcd_reconnect_delay: u64,
}


#[rocket::main]
async fn main() -> ExitCode {
    let config = config::init();
    logger::init(&config);
    let server_config: rocket::figment::Figment = config.borrow().into();

    //etcd watcher
    let args = Args::parse();
    
    // Initialize Cedar structures with proper initialization
    let policy_set = Arc::new(RwLock::new(cedar_policy::PolicySet::new()));
    // Fix Schema initialization with proper serde_json import
    let schema = Arc::new(RwLock::new(
        cedar_policy::Schema::from_json_value(serde_json::json!({"": {}}))
            .unwrap_or_else(|_| {
                log::warn!("Failed to create empty schema, using default");
                cedar_policy::Schema::from_schema_fragments(vec![]).unwrap()
            })
    ));
    let entities = Arc::new(RwLock::new(cedar_policy::Entities::empty()));
    
    // Start etcd watcher if enabled
    if args.enable_etcd_watcher {
        let endpoints = args.etcd_endpoints
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();
        
        let watcher_config = EtcdWatcherConfig {
            endpoints,
            reconnect_delay_secs: args.etcd_reconnect_delay,
        };
        
        match EtcdWatcher::new(
            policy_set.clone(), 
            schema.clone(), 
            entities.clone(),
            watcher_config
        ).await {
            Ok(watcher) => {
                log::info!("Starting etcd watcher");
                let _handles = watcher.start_watching().await;
                // We don't need to join these handles as they run indefinitely
            }
            Err(e) => {
                log::error!("Failed to initialize etcd watcher: {}", e);
                // You might want to fail here or continue without the watcher
            }
        }
    }
    //end etcd watcher

    // Rest of your code remains the same
    let launch_result = rocket::custom(server_config)
        .attach(common::DefaultContentType::new(ContentType::JSON))
        .attach(services::schema::load_from_file::InitSchemaFairing)
        .attach(services::data::load_from_file::InitDataFairing)
        .attach(services::policies::load_from_file::InitPoliciesFairing)
        .manage(config)
        .manage(Box::new(MemoryPolicyStore::new()) as Box<dyn PolicyStore>)
        .manage(Box::new(MemoryDataStore::new()) as Box<dyn DataStore>)
        .manage(Box::new(MemorySchemaStore::new()) as Box<dyn SchemaStore>)
        .manage(cedar_policy::Authorizer::new())
        .register(
            "/",
            catchers![
                errors::catchers::handle_500,
                errors::catchers::handle_404,
                errors::catchers::handle_400,
            ],
        )
        .mount("/", openapi_get_routes![routes::health,])
        .mount("/health", openapi_get_routes![routes::health,])
        .mount(
            "/v1",
            openapi_get_routes![
                routes::health,
                routes::policies::get_policies,
                routes::policies::get_policy,
                routes::policies::create_policy,
                routes::policies::update_policies,
                routes::policies::update_policy,
                routes::policies::delete_policy,
                routes::data::get_entities,
                routes::data::update_entities,
                routes::data::delete_entities,
                routes::authorization::is_authorized,
                routes::schema::get_schema,
                routes::schema::update_schema,
                routes::schema::delete_schema
            ],
        )
        .mount(
            "/swagger-ui/",
            make_swagger_ui(&SwaggerUIConfig {
                url: "../v1/openapi.json".to_owned(),
                ..Default::default()
            }),
        )
        .mount(
            "/rapidoc/",
            make_rapidoc(&RapiDocConfig {
                general: GeneralConfig {
                    spec_urls: vec![UrlObject::new("General", "../v1/openapi.json")],
                    ..Default::default()
                },
                hide_show: HideShowConfig {
                    allow_spec_url_load: false,
                    allow_spec_file_load: false,
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
        .launch()
        .await;
    return match launch_result {
        Ok(_) => {
            info!("Cedar-Agent shut down gracefully.");
            ExitCode::SUCCESS
        }
        Err(err) => {
            error!("Cedar-Agent shut down with error: {}", err);
            ExitCode::FAILURE
        }
    };
}