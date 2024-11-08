use crate::{RPCResponse, RPCResult};
use serde::de::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::time::Duration;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv6Addr},
};

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct RequestHeader {
    pub seq: u64,
    pub command: &'static str,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ResponseHeader {
    pub seq: u64,
    pub error: String,
}

macro_rules! count {
    () => { 0 };
    ($item:tt) => {1};
    ($item:tt$(, $rest:tt)+) => { count!( $($rest),+ ) + 1 }
}

macro_rules! cmd_arg {
    (
        $buf:expr,
        $($key:literal: $val:expr),*
    ) => {{
        let len: u32 = count!( $($key),* );

        rmp::encode::write_map_len($buf, len).unwrap();
        $(
            rmp::encode::write_str($buf, $key).unwrap();
            rmp_serde::encode::write_named($buf, $val).unwrap();
        )*
    }};
}

macro_rules! req {
    (
        $name:literal
        $(#[$meta:meta])*
        $vis:vis $ident:ident( $($arg:ident: $arg_ty:ty),* ) -> $res:ty $({
            $($key:literal: $val:expr),*
        })?
    ) => {
        impl crate::Client {
            $(#[$meta])*
            $vis fn $ident(&self$(, $arg: $arg_ty)*) -> crate::RPCRequest<$res> {
                #[allow(unused_mut)]
                let mut buf = Vec::new();

                $(cmd_arg! { &mut buf, $($key: $val),* };)?

                self.request($name, buf)
            }
        }
    };
}

macro_rules! stream {
    (
        $name:literal
        $(#[$meta:meta])*
        $vis:vis $ident:ident( $($arg:ident: $arg_ty:ty),* ) -> $res:ty $({
            $($key:literal: $val:expr),*
        })?
    ) => {
        impl crate::Client {
            $(#[$meta])*
            $vis fn $ident(self: &std::sync::Arc<Self>$(, $arg: $arg_ty)*) -> crate::RPCStream<$res> {
                #[allow(unused_mut)]
                let mut buf = Vec::new();

                $(cmd_arg! { &mut buf, $($key: $val),* };)?

                self.start_stream($name, buf)
            }
        }
    };
}

macro_rules! res {
    ($ty:ty) => {
        impl RPCResponse for $ty {
            fn read_from(read: crate::SeqRead<'_>) -> RPCResult<Self> {
                Ok(read.read_msg())
            }
        }
    };
}

req! {
    "handshake"
    /// Send a handshake
    pub(crate) handshake(version: u32) -> () {
        "Version": &version
    }
}

req! {
    "auth"
    /// Send an auth key
    pub(crate) auth(auth_key: &str) -> () {
        "AuthKey": auth_key
    }
}

req! {
    "event"
    /// Fire an event
    pub fire_event(name: &str, payload: &[u8], coalesce: bool) -> () {
        "Name": name,
        "Payload": payload,
        "Coalesce": &coalesce
    }
}

req! {
    "force-leave"
    /// Force a node to leave
    pub force_leave(node: &str) -> () {
        "Node": node
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct JoinResponse {
    #[serde(rename = "Num")]
    pub nodes_joined: u64,
}

res!(JoinResponse);

req! {
    "join"
    /// Join a serf cluster, given existing ip addresses. `replay` controls whether to replay old user events
    pub join(existing: &[&str], replay: bool) -> JoinResponse {
        "Existing": existing,
        "Replay": &replay
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Member {
    pub name: String,
    #[serde(deserialize_with = "deserialize_ip_addr")]
    pub addr: IpAddr,
    pub port: u32,
    pub tags: HashMap<String, String>,
    pub status: String,
    pub protocol_min: u32,
    pub protocol_max: u32,
    pub protocol_cur: u32,
    pub delegate_max: u32,
    pub delegate_min: u32,
    pub delegate_cur: u32,
}

fn deserialize_ip_addr<'de, D>(de: D) -> Result<IpAddr, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let bytes: Vec<u8> = serde_bytes::deserialize(de)?;

    let maybe_ipv6: Result<[u8; 16], _> = bytes.try_into();
    match maybe_ipv6 {
        Ok(ipv6) => {
            let ipv6 = Ipv6Addr::from(ipv6);
            match ipv6.to_ipv4() {
                None => Ok(ipv6.into()),
                Some(ipv4) => Ok(ipv4.into()),
            }
        }
        Err(bytes) => {
            let maybe_ipv4: Result<[u8; 4], _> = bytes.try_into();
            match maybe_ipv4 {
                Ok(ipv4) => Ok(ipv4.into()),
                Err(bytes) => Err(D::Error::custom(format!(
                    "Could not parse {bytes:?} into IP address"
                ))),
            }
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct MembersResponse {
    pub members: Vec<Member>,
}

res!(MembersResponse);

req! {
    "members"
    /// Returns a list of all known members
    pub members() -> MembersResponse
}

req! {
    "members-filtered"
    /// Returns a filtered list of all known members
    pub members_filtered(status: Option<&str>, name: Option<&str>, tags: Option<&HashMap<String, String>>) -> MembersResponse {
        "Status": &status,
        "Name": &name,
        "Tags": &tags
    }
}

req! {
    "tags"
    /// Modifies the tags of the current node
    pub tags(add_tags: &HashMap<String, String>, delete_tags: &[&str]) -> MembersResponse {
        "Tags": add_tags,
        "DeleteTags": delete_tags
    }
}

req! {
    "stop"
    /// Stops a stream by seq id (this is automatically called on Drop by the RPCStream struct)
    pub(crate) stop_stream(seq: u64) -> () {
        "Stop": &seq
    }
}

req! {
    "leave"
    /// Gracefully leave
    pub leave() -> ()
}

req! {
    "respond"
    /// Response to a query
    pub query_respond(id: u64, payload: &[u8]) -> () {
        "ID": &id,
        "Payload": payload
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Coordinate {
    pub adjustment: f32,
    pub error: f32,
    pub height: f32,
    pub vec: [f32; 8],
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct CoordinateResponse {
    pub ok: bool,

    #[serde(default)]
    pub coord: Option<Coordinate>,
}

res!(CoordinateResponse);

req! {
    "get-coordinate"
    /// Get a node's coordinate
    pub get_coordinate(node: &str) -> CoordinateResponse {
        "Node": node
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Agent {
    pub name: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RuntimeInfo {
    pub os: String,
    pub arch: String,
    pub version: String,
    pub max_procs: String,
    pub goroutines: String,
    pub cpu_count: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SerfInfo {
    pub failed: String,
    pub left: String,
    pub event_time: String,
    pub query_time: String,
    pub event_queue: String,
    pub members: String,
    pub member_time: String,
    pub intent_queue: String,
    pub query_queue: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AgentStats {
    pub agent: Agent,
    pub runtime: RuntimeInfo,
    pub serf: SerfInfo,
    pub tags: HashMap<String, String>,
}

res!(AgentStats);

req! {
    "stats"
    /// Get information about the Serf agent.
    pub stats() -> AgentStats
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "Event")]
pub enum StreamMessage {
    #[serde(rename = "user")]
    User {
        #[serde(rename = "LTime")]
        ltime: u64,
        #[serde(rename = "Name")]
        name: String,
        #[serde(rename = "Payload")]
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
        #[serde(rename = "Coalesce")]
        coalesce: bool,
    },
    #[serde(rename = "member-join")]
    MemberJoin {
        #[serde(rename = "Members")]
        members: Vec<Member>,
    },
    #[serde(rename = "member-update")]
    MemberUpdate {
        #[serde(rename = "Members")]
        members: Vec<Member>,
    },
    #[serde(rename = "member-leave")]
    MemberLeave {
        #[serde(rename = "Members")]
        members: Vec<Member>,
    },
    #[serde(rename = "member-failed")]
    MemberFail {
        #[serde(rename = "Members")]
        members: Vec<Member>,
    },
    #[serde(rename = "query")]
    Query {
        #[serde(rename = "ID")]
        id: u64,
        #[serde(rename = "LTime")]
        ltime: u64,
        #[serde(rename = "Name")]
        name: String,
        #[serde(rename = "Payload")]
        #[serde(with = "serde_bytes")]
        payload: Option<Vec<u8>>,
    },
}
res!(StreamMessage);

stream! {
    "stream"
    pub stream(ty: &str) -> StreamMessage {
        "Type": ty
    }
}

#[derive(Deserialize, Debug)]
#[serde(tag = "Type")]
pub enum QueryResponse {
    #[serde(rename = "ack")]
    Ack {
        #[serde(rename = "From")]
        from: String,
    },
    #[serde(rename = "response")]
    Response {
        #[serde(rename = "From")]
        from: String,
        #[serde(rename = "Payload")]
        #[serde(with = "serde_bytes")]
        payload: Option<Vec<u8>>,
    },
    #[serde(rename = "done")]
    Done,
}
res!(QueryResponse);

#[derive(Default, Debug)]
pub struct QueryParams {
    pub request_ack: Option<bool>,
    pub relay_factor: Option<u8>,
    pub timeout: Option<Duration>,
    pub filter_nodes: Option<Vec<String>>,
    pub filter_tags: Option<HashMap<String, String>>,
}

stream! {
    "query"
    /// The query command dispatches a custom user query into a Serf cluster, efficiently
    /// broadcasting the query to all nodes, and gathering responses.
    /// <https://github.com/hashicorp/serf/blob/master/docs/commands/query.html.markdown>
    /// <https://github.com/hashicorp/serf/blob/master/docs/agent/rpc.html.markdown#query>
    pub query(
        name: &str,
        payload: &[u8],
        params: QueryParams
    ) -> QueryResponse {
        "FilterNodes": &params.filter_nodes,
        "FilterTags": &params.filter_tags,
        "Timeout": &params.timeout.map(|x| x.as_nanos()),
        "RequestAck": &params.request_ack.unwrap_or(true),
        "RelayFactor": &params.relay_factor.unwrap_or_default(),
        "Name": name,
        "Payload": payload
    }
}
