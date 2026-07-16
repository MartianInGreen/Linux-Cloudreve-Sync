use anyhow::Result;
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};

#[zbus::proxy(
    interface = "org.freedesktop.login1.Manager",
    default_service = "org.freedesktop.login1",
    default_path = "/org/freedesktop/login1"
)]
trait LoginManager {
    fn inhibit(
        &self,
        what: &str,
        who: &str,
        why: &str,
        mode: &str,
    ) -> zbus::Result<zbus::zvariant::OwnedFd>;

    #[zbus(signal)]
    fn prepare_for_sleep(&self, start: bool) -> zbus::Result<()>;
}

#[derive(Debug)]
pub enum PowerEvent {
    Suspending(oneshot::Sender<()>),
    Resumed,
}

pub async fn monitor(events: mpsc::UnboundedSender<PowerEvent>) -> Result<()> {
    let connection = zbus::Connection::system().await?;
    let proxy = LoginManagerProxy::new(&connection).await?;
    let mut signals = proxy.receive_prepare_for_sleep().await?;
    let mut inhibitor = Some(
        proxy
            .inhibit(
                "sleep",
                "cloudreve-sync",
                "Pause synchronization safely",
                "delay",
            )
            .await?,
    );
    while let Some(signal) = signals.next().await {
        if signal.args()?.start {
            let (ack_tx, ack_rx) = oneshot::channel();
            if events.send(PowerEvent::Suspending(ack_tx)).is_err() {
                break;
            }
            let _ = tokio::time::timeout(std::time::Duration::from_secs(4), ack_rx).await;
            inhibitor.take();
            continue;
        }
        if events.send(PowerEvent::Resumed).is_err() {
            break;
        }
        inhibitor = proxy
            .inhibit(
                "sleep",
                "cloudreve-sync",
                "Pause synchronization safely",
                "delay",
            )
            .await
            .ok();
    }
    drop(inhibitor);
    Ok(())
}
