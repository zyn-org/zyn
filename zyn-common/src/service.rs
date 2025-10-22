// SPDX-License-Identifier: AGPL-3.0

use std::fmt::Debug;

/// Trait for service type identification
pub trait Service: Clone + Debug + Send + Sync + 'static {
  /// Returns the service type name
  const NAME: &'static str;
}

/// Marker type for C2S service
#[derive(Clone, Debug)]
pub struct C2sService;

impl Service for C2sService {
  const NAME: &'static str = "c2s";
}

/// Marker type for S2M service
#[derive(Clone, Debug)]
pub struct S2mService;

impl Service for S2mService {
  const NAME: &'static str = "s2m";
}

/// Marker type for M2S service
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct M2sService;

impl Service for M2sService {
  const NAME: &'static str = "m2s";
}
