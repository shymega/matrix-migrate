use std::time::Duration;

use clap::Parser;
use futures::{future::{try_join, join_all, try_join_all},
              try_join, pin_mut, StreamExt};
use log::{info, warn};
use matrix_sdk::{
    config::SyncSettings,
    ruma::{OwnedRoomId, OwnedServerName, OwnedUserId},
    Client,
};

/// Fast migration of one matrix account to another
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Username of the account to migrate from
    #[arg(long = "from", env = "FROM_USER")]
    from_user: OwnedUserId,

    /// Password of the account to migrate from
    #[arg(long = "from-pw", env = "FROM_PASSWORD")]
    from_user_password: Option<String>,

    /// Username of the given account to migrate to
    #[arg(long = "to", env = "TO_USER")]
    to_user: OwnedUserId,

    /// Password of the account to migrate from
    #[arg(long = "to-pw", env = "TO_PASSWORD")]
    to_user_password: Option<String>,

    /// Custom timeout for syncing, default is 10 secs
    #[arg(long, env = "TIMEOUT")]
    timeout: Option<u64>,
}

use anyhow::Result;

#[derive(Debug, Clone)]
pub struct State {
    pub user_id: Box<OwnedUserId>,
    inner_client: Option<Box<Client>>,
}

impl State {
    pub fn new(user_id: Box<OwnedUserId>) -> Self {
        Self {
            user_id,
            inner_client: None,
        }
    }

    pub fn add_client(&mut self, client: Client) -> Result<()> {
        self.inner_client = Option::from(Box::from(client));
        Ok(())
    }

    pub fn get_client(&mut self) -> Client {
        *self.inner_client.clone().unwrap()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    let mut from_state = State::new(Box::new(args.from_user));
    from_state.add_client(
        Client::builder()
            .server_name(
                &*from_state.user_id.server_name())
            .build()
            .await.unwrap()).unwrap();

    info!("Logging in {:}", from_state.user_id.to_string());

    if args.from_user_password.is_none() {
        info!("No password provided, trying SSO authentication.");
        from_state
            .get_client()
            .matrix_auth()
            .login_sso(|sso_url| async move {
                info!("Opening URL automatically, if it fails, use the following link: {sso_url}");
                open::that(sso_url)?;
                Ok(())
            })
            .initial_device_display_name("matrix-migrate")
            .await?;
    } else {
        from_state
            .get_client()
            .matrix_auth()
            .login_username(
                from_state.user_id.localpart(),
                &args.from_user_password.unwrap(),
            )
            .await?;
    };

    let mut to_state = State::new(Box::new(args.to_user));
    to_state.add_client(
        Client::builder()
            .server_name(
                &*to_state.user_id.server_name())
            .homeserver_url(&format!("https://{}", to_state.user_id.server_name()))
            .build()
            .await.unwrap()).unwrap();

    info!("Logging in {:}", to_state.user_id.to_string());

    if args.to_user_password.is_none() {
        info!("No password provided, trying SSO authentication.");
        to_state
            .get_client()
            .matrix_auth()
            .login_sso(|sso_url| async move {
                info!("Open this URL in a web browser {:}", sso_url);
                open::that(sso_url)?;
                Ok(())
            })
            .initial_device_display_name("matrix-migrate")
            .await?;
    } else {
        to_state
            .get_client()
            .matrix_auth()
            .login_username(
                to_state.user_id.localpart(),
                &args.to_user_password.unwrap(),
            )
            .await?;
    }

    let to_client = to_state.clone().get_client().clone();
    let from_client = from_state.clone().get_client().clone();

    info!("All logged in. Syncing...");

    let settings = if let Some(s) = args.timeout {
        SyncSettings::default().timeout(Duration::from_secs(s))
    } else {
        SyncSettings::default()
            .timeout(Duration::from_secs(3600))
    };


    let to_stream = to_client
        .sync_stream(settings.clone())
        .await;
    let from_stream = from_client
        .sync_once(settings.clone());

    pin_mut!(to_stream);
    try_join!(from_stream, async {
            to_stream.next().await.unwrap()
        })?;

    info!("We are now synced!");

    let prev_rooms = from_state
        .clone()
        .get_client()
        .clone()
        .joined_rooms()
        .into_iter()
        .map(|r| r.room_id().to_owned())
        .collect::<Vec<_>>();

    let new_rooms = to_state
        .clone()
        .get_client()
        .joined_rooms()
        .into_iter()
        .map(|r| r.room_id().to_owned())
        .chain(
            to_state
                .clone()
                .get_client()
                .invited_rooms()
                .into_iter()
                .map(|r| r.room_id().to_owned()),
        )
        .collect::<Vec<_>>();

    let (already_invited, to_invite): (Vec<_>, Vec<_>) =
        prev_rooms.iter().partition(|r| new_rooms.contains(r));

    let invites_to_accept = to_state
        .get_client()
        .invited_rooms()
        .into_iter()
        .filter_map(|r| {
            let room_id = r.room_id().to_owned();
            if prev_rooms.contains(&room_id) {
                Some(room_id)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let to_user = to_state.clone().user_id;
    let to_accept = invites_to_accept.iter().collect();
    let c_accept = to_client.clone();
    let ensure_user = to_user.clone();
    let ensure_c = from_state.get_client().clone();
    let inviter_c = from_state.get_client().clone();

    let (_, not_yet_accepted, (remaining_invites, failed_invites)) = try_join!(
        async move { ensure_power_levels(&ensure_c, *ensure_user, &already_invited).await },
        async move { accept_invites(&c_accept, &to_accept).await },
        async move {
            let to_invite = to_invite.clone();
            let failed_invites = send_invites(&inviter_c, &to_invite, *to_user.clone()).await?;
            ensure_power_levels(&inviter_c, *to_user, &to_invite).await?;
            Ok((
                to_invite
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .filter(|r| !failed_invites.contains(r))
                    .collect::<Vec<_>>(),
                failed_invites,
            ))
        },
    )?;

    let mut invites_awaiting = not_yet_accepted
        .into_iter()
        .chain(remaining_invites.into_iter())
        .collect::<Vec<_>>();

    info!("First invitation set done.");
    while !invites_awaiting.is_empty() {
        info!("Still {} rooms to go. Syncing up", invites_awaiting.len());
        to_stream.next().await.expect("Sync stream failed").unwrap();
        invites_awaiting =
            accept_invites(&to_state.get_client(), &invites_awaiting.iter().collect()).await?;
    }

    if !failed_invites.is_empty() {
        warn!(
            "Failed to invite to {:?}. See logs above for the reasons why",
            failed_invites
        );
    }

    info!("-- All done! -- ");

    Ok(())
}

async fn ensure_power_levels(
    from_c: &Client,
    new_username: OwnedUserId,
    rooms: &Vec<&OwnedRoomId>,
) -> Result<()> {
    try_join_all(rooms.iter().enumerate().map(|(counter, room_id)| {
        let from_c = from_c.clone();
        let self_id = from_c.user_id().unwrap().to_owned();
        let user_id = new_username.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(counter.saturating_div(2) as u64)).await;
            let Some(joined) = from_c.get_room(&room_id) else {
                return anyhow::Ok(());
            };

            let Some(me) = joined.get_member(&self_id).await? else {
                warn!("{self_id} isn't member of {room_id}. Skipping power_level ensuring.");
                return anyhow::Ok(());
            };

            let Some(new_acc) = joined.get_member(&user_id).await? else {
                warn!("{user_id} isn't member of {room_id}. Skipping power_level ensuring.");
                return anyhow::Ok(());
            };

            let my_power_level = me.power_level();

            if my_power_level <= new_acc.power_level() {
                info!("Power levels of {user_id} and {self_id} in {room_id} are fine.");
                return anyhow::Ok(());
            }

            info!("Trying to adjust power_level of {user_id} in {room_id} to {my_power_level}.");

            if let Err(e) = joined
                .update_power_levels(vec![(&user_id.clone(), my_power_level.try_into().unwrap())])
                .await
            {
                warn!("Couldn't update power levels for {user_id} in {room_id}: {e}");
            }

            Ok(())
        }
    }))
    .await?;
    Ok(())
}

async fn accept_invites(to_c: &Client, rooms: &Vec<&OwnedRoomId>) -> Result<Vec<OwnedRoomId>> {
    let mut pending = Vec::new();
    for room_id in rooms {
        let Some(invited) = to_c.get_room(&room_id) else {
            if to_c.get_room(room_id).is_some() {
                // already existing, skipping
                continue;
            }
            pending.push(room_id.clone().to_owned());
            continue;
        };
        info!(
            "Accepting invite for {}({})",
            invited.display_name().await?,
            invited.room_id()
        );
        to_c.join_room_by_id(invited.room_id()).await?;
    }

    Ok(pending)
}

async fn send_invites(
    from_c: &Client,
    rooms: &Vec<&OwnedRoomId>,
    user_id: OwnedUserId,
) -> Result<Vec<OwnedRoomId>> {
    Ok(join_all(rooms.iter().enumerate().map(|(counter, room_id)| {
        let from_c = from_c.clone();
        let user_id = user_id.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(counter.saturating_div(2) as u64)).await;
            let Some(joined) = from_c.get_room(&room_id) else {
                warn!("Can't invite user to {:}: not a member myself", room_id);
                return Some(room_id.clone().to_owned());
            };
            info!(
                "Inviting to {room_id} ({})",
                joined.display_name().await.unwrap()
            );
            if let Err(e) = joined.invite_user_by_id(&user_id).await {
                warn!("Inviting to {:} failed: {e}", room_id);
                return Some(room_id.clone().to_owned());
            }
            None
        }
    }))
    .await
    .into_iter()
    .filter_map(|e| e)
    .collect())
}
