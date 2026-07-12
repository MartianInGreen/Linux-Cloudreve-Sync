use anyhow::{bail, Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::{Client, Method, StatusCode};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use url::Url;

#[derive(Clone, Debug)]
pub struct RemoteEntry {
    pub tag: String,
    pub is_dir: bool,
    pub relative_path: String,
}

#[derive(Clone)]
pub struct WebDavClient {
    client: Client,
    base: Url,
    username: String,
    password: String,
}

impl WebDavClient {
    pub fn new(base: &str, username: &str, password: &str) -> Result<Self> {
        let base = Url::parse(&format!("{}/", base.trim_end_matches('/')))
            .context("WebDAV URL is invalid")?;
        Ok(Self {
            client: Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(60))
                .build()?,
            base,
            username: username.into(),
            password: password.into(),
        })
    }

    fn url(&self, path: &str) -> Result<Url> {
        self.base
            .join(path.trim_start_matches('/'))
            .context("invalid remote path")
    }

    fn request(&self, method: Method, path: &str) -> Result<reqwest::RequestBuilder> {
        Ok(self
            .client
            .request(method, self.url(path)?)
            .basic_auth(&self.username, Some(&self.password)))
    }

    pub async fn test(&self) -> Result<()> {
        let response = self
            .request(Method::from_bytes(b"PROPFIND")?, "")?
            .header("Depth", "0")
            .send()
            .await?;
        if response.status() != StatusCode::MULTI_STATUS && !response.status().is_success() {
            bail!("server returned {}", response.status());
        }
        Ok(())
    }

    pub async fn list_recursive(&self, root: &str) -> Result<BTreeMap<String, RemoteEntry>> {
        let mut result = BTreeMap::new();
        let mut pending = vec![root.trim_matches('/').to_string()];
        let mut visited = BTreeSet::new();
        while let Some(directory) = pending.pop() {
            let directory = normalize_remote_path(&directory);
            if !visited.insert(directory.clone()) {
                continue;
            }
            for (name, entry) in self.list_one(&directory).await? {
                let name = normalize_remote_path(&name);
                // Depth: 1 includes the requested directory; do not queue it again.
                if name == directory {
                    continue;
                }
                let relative = name
                    .strip_prefix(root.trim_matches('/'))
                    .unwrap_or(&name)
                    .trim_matches('/')
                    .to_string();
                if relative.is_empty() {
                    continue;
                }
                if entry.is_dir {
                    pending.push(name.clone());
                }
                result.insert(
                    relative.clone(),
                    RemoteEntry {
                        relative_path: relative,
                        ..entry
                    },
                );
            }
        }
        Ok(result)
    }

    async fn list_one(&self, path: &str) -> Result<Vec<(String, RemoteEntry)>> {
        let response = self
            .request(Method::from_bytes(b"PROPFIND")?, path)?
            .header("Depth", "1")
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        if response.status() != StatusCode::MULTI_STATUS && !response.status().is_success() {
            bail!("listing {path} returned {}", response.status());
        }
        parse_multistatus(&response.bytes().await?, &self.base)
    }

    pub async fn download(&self, path: &str) -> Result<Vec<u8>> {
        let response = self
            .request(Method::GET, path)?
            .send()
            .await?
            .error_for_status()?;
        Ok(response.bytes().await?.to_vec())
    }

    pub async fn upload(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.ensure_parents(path).await?;
        self.request(Method::PUT, path)?
            .body(data)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let response = self.request(Method::DELETE, path)?.send().await?;
        if !response.status().is_success() && response.status() != StatusCode::NOT_FOUND {
            bail!("deleting remote file returned {}", response.status());
        }
        Ok(())
    }

    async fn ensure_parents(&self, path: &str) -> Result<()> {
        let parts: Vec<_> = path.trim_matches('/').split('/').collect();
        let mut current = String::new();
        for part in parts.iter().take(parts.len().saturating_sub(1)) {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(part);
            let response = self
                .request(Method::from_bytes(b"MKCOL")?, &current)?
                .send()
                .await?;
            if !response.status().is_success()
                && response.status() != StatusCode::METHOD_NOT_ALLOWED
            {
                bail!(
                    "creating remote directory {current} returned {}",
                    response.status()
                );
            }
        }
        Ok(())
    }
}

fn parse_multistatus(body: &[u8], base: &Url) -> Result<Vec<(String, RemoteEntry)>> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut entries = Vec::new();
    let (mut href, mut tag, mut is_dir) = (None, None, false);
    loop {
        match reader.read_event()? {
            Event::Start(e) | Event::Empty(e) => match e.local_name().as_ref() {
                b"response" => {
                    href = None;
                    tag = None;
                    is_dir = false;
                }
                b"href" => href = Some(reader.read_text(e.name())?.into_owned()),
                b"getetag" => tag = Some(reader.read_text(e.name())?.into_owned()),
                b"collection" => is_dir = true,
                _ => {}
            },
            Event::End(e) if e.local_name().as_ref() == b"response" => {
                if let Some(raw) = href.take() {
                    let decoded = percent_decode(&raw);
                    let base_path = percent_decode(base.path())
                        .trim_end_matches('/')
                        .to_string();
                    let path = decoded
                        .strip_prefix(&base_path)
                        .unwrap_or(&decoded)
                        .trim_matches('/')
                        .to_string();
                    entries.push((
                        path,
                        RemoteEntry {
                            tag: tag.take().unwrap_or_default(),
                            is_dir,
                            relative_path: String::new(),
                        },
                    ));
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(entries)
}

fn percent_decode(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8_lossy()
        .into_owned()
}

fn normalize_remote_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_response_matches_requested_directory() {
        assert_eq!(
            normalize_remote_path("/documents/work/"),
            normalize_remote_path("documents/work")
        );
    }
}
