use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusType {
    /// Indicates service status is unknown
    Unknown,
    /// Indicates service is serving data
    Serving,
    /// Indicates that the service is down.
    Down,
    /// Indicates that the service not serving data, but the service is not down.
    // TODO(ramfox): NotServing is currently unused
    NotServing,
}

pub const HEALTH_POLL_WAIT: Duration = std::time::Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
/// The status of an individual rpc service
pub struct ServiceStatus {
    typ: ServiceType,
    status: StatusType,
    version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceType {
    P2p,
    Store,
}

impl ServiceType {
    pub fn name(&self) -> &'static str {
        match self {
            ServiceType::P2p => "relay",
            ServiceType::Store => "store",
        }
    }
}

impl ServiceStatus {
    pub fn new<I: Into<String>>(typ: ServiceType, status: StatusType, version: I) -> Self {
        Self {
            typ,
            status,
            version: version.into(),
        }
    }

    pub fn name(&self) -> &'static str {
        self.typ.name()
    }

    pub fn status(&self) -> StatusType {
        self.status.clone()
    }

    pub fn version(&self) -> &str {
        if self.version.is_empty() {
            "unknown"
        } else {
            &self.version
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientStatus {
    pub p2p: ServiceStatus,
    pub store: ServiceStatus,
}

impl ClientStatus {
    pub fn new(p2p: Option<ServiceStatus>, store: Option<ServiceStatus>) -> Self {
        Self {
            p2p: p2p
                .unwrap_or_else(|| ServiceStatus::new(ServiceType::P2p, StatusType::Unknown, "")),
            store: store
                .unwrap_or_else(|| ServiceStatus::new(ServiceType::Store, StatusType::Unknown, "")),
        }
    }

    pub fn iter(&self) -> ClientStatusIterator<'_> {
        ClientStatusIterator {
            table: self,
            iter: 0,
        }
    }

    pub fn update(&mut self, s: ServiceStatus) {
        match s.typ {
            ServiceType::P2p => self.p2p = s,
            ServiceType::Store => self.store = s,
        }
    }
}

#[derive(Debug)]
pub struct ClientStatusIterator<'a> {
    table: &'a ClientStatus,
    iter: usize,
}

impl Iterator for ClientStatusIterator<'_> {
    type Item = ServiceStatus;

    fn next(&mut self) -> Option<Self::Item> {
        let current = match self.iter {
            0 => Some(self.table.store.to_owned()),
            1 => Some(self.table.p2p.to_owned()),
            _ => None,
        };

        self.iter += 1;
        current
    }
}

impl Default for ClientStatus {
    fn default() -> Self {
        Self::new(None, None)
    }
}
