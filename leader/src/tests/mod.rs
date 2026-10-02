//! De tests van `internal/leader` uit de Go-generatie (github.com/xinix00/hop,
//! tag v1.0.7), per Go-bestand één module, met dezelfde namen in snake_case.
//!
//! Go wachtte met `time.Sleep` tot goroutines klaar waren; hier is alles
//! synchroon, dus een bewering volgt direct op de handeling.

mod basic;
mod edge_cases;
mod failover;
mod leader_restart;
mod persist_init;
mod phantom_node;
mod placed_reporting;
mod priority;
mod robustness;
mod scenarios;
mod self_registration;
mod update;
