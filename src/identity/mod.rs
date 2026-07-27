//! Identity domain contracts.
//!
//! This module defines the portable, backend-agnostic types for Citadel
//! accounts and authentication credentials. It follows the same contract-first
//! precedent as [`crate::storage`]: strongly typed value objects with validating
//! constructors, no raw strings at boundaries, and no dependency on a concrete
//! database or transport.
//!
//! Account identity is [`crate::storage::UserId`], re-exported here so identity,
//! storage ownership, and future services all use one id type rather than
//! competing definitions.
//!
//! Scope is device and custom auth. Email/password, social providers, and JWT
//! signing are deliberately out of scope and slot in behind [`AuthCredential`]
//! and the service traits in [`crate::services`] without reshaping this contract.

pub mod auth;
pub mod user;

pub use auth::{
    AuthCredential, AuthIdentity, AuthProvider, CustomId, DeviceId, EmailAddress, Password,
    PasswordVerifier,
};
pub use user::{AccountState, DisplayName, User, UserMetadata, Username};

// Account identity is the storage user id; do not define a second `UserId`.
pub use crate::storage::UserId;
