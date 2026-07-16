use anyhow::{bail, Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::{
    header::{IF_MATCH, IF_NONE_MATCH, RETRY_AFTER},
    Client, Method, Response, StatusCode,
};
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

pub enum CollectionListing {
    Found(BTreeMap<String, RemoteEntry>),
    Missing,
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
        if !path.is_empty() {
            validate_remote_path(path)?;
        }
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("WebDAV URL cannot contain remote paths"))?
            .pop_if_empty()
            .extend(
                path.trim_matches('/')
                    .split('/')
                    .filter(|part| !part.is_empty()),
            );
        Ok(url)
    }

    fn request(&self, method: Method, path: &str) -> Result<reqwest::RequestBuilder> {
        Ok(self
            .client
            .request(method, self.url(path)?)
            .basic_auth(&self.username, Some(&self.password)))
    }

    pub async fn test(&self) -> Result<()> {
        let response = self
            .send_read_with_retry(Method::from_bytes(b"PROPFIND")?, "", Some("0"), None)
            .await?;
        if response.status() != StatusCode::MULTI_STATUS && !response.status().is_success() {
            bail!("server returned {}", response.status());
        }
        Ok(())
    }

    pub async fn is_reachable(&self) -> Result<bool> {
        match self
            .request(Method::from_bytes(b"PROPFIND")?, "")?
            .header("Depth", "0")
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if error.is_connect() || error.is_timeout() => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn list_recursive(&self, root: &str) -> Result<CollectionListing> {
        let mut result = BTreeMap::new();
        let root = normalize_remote_path(root);
        if !root.is_empty() {
            validate_remote_path(&root)?;
        }
        let mut pending = vec![root.clone()];
        let mut visited = BTreeSet::new();
        while let Some(directory) = pending.pop() {
            let directory = normalize_remote_path(&directory);
            if !visited.insert(directory.clone()) {
                continue;
            }
            let Some(entries) = self.list_one(&directory).await? else {
                if directory == root {
                    return Ok(CollectionListing::Missing);
                }
                bail!("remote directory {directory} disappeared while it was being listed");
            };
            for (name, entry) in entries {
                let name = normalize_remote_path(&name);
                // Depth: 1 includes the requested directory; do not queue it again.
                if name == directory {
                    continue;
                }
                validate_remote_path(&name)?;
                let relative = if root.is_empty() {
                    name.clone()
                } else {
                    relative_to_root(&name, &root)?
                };
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
        Ok(CollectionListing::Found(result))
    }

    async fn list_one(&self, path: &str) -> Result<Option<Vec<(String, RemoteEntry)>>> {
        for attempt in 0..3 {
            let response = self
                .send_read_with_retry(Method::from_bytes(b"PROPFIND")?, path, Some("1"), None)
                .await
                .with_context(|| format!("requesting WebDAV listing for {path}"))?;
            if response.status() == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if response.status() != StatusCode::MULTI_STATUS && !response.status().is_success() {
                bail!("listing {path} returned {}", response.status());
            }
            match response.bytes().await {
                Ok(body) => return parse_multistatus(&body, &self.base).map(Some),
                Err(_) if attempt < 2 => {
                    tokio::time::sleep(Duration::from_millis(250 * (attempt as u64 + 1))).await;
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("reading WebDAV listing response for {path}"));
                }
            }
        }
        unreachable!()
    }

    pub async fn download(&self, path: &str, expected_tag: Option<&str>) -> Result<Vec<u8>> {
        for attempt in 0..3 {
            let response = self
                .send_read_with_retry(Method::GET, path, None, expected_tag)
                .await
                .with_context(|| format!("requesting remote file {path}"))?
                .error_for_status()
                .with_context(|| format!("downloading remote file {path}"))?;
            match response.bytes().await {
                Ok(body) => return Ok(body.to_vec()),
                Err(_) if attempt < 2 => {
                    tokio::time::sleep(Duration::from_millis(250 * (attempt as u64 + 1))).await;
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("reading remote file response for {path}"));
                }
            }
        }
        unreachable!()
    }

    pub async fn upload(
        &self,
        path: &str,
        data: Vec<u8>,
        expected_tag: Option<&str>,
    ) -> Result<StatusCode> {
        self.ensure_parents(path).await?;
        let mut request = self.request(Method::PUT, path)?.body(data);
        request = match expected_tag {
            Some(tag) if !tag.is_empty() => request.header(IF_MATCH, tag),
            Some(_) => bail!("cannot safely replace a remote file without an ETag"),
            None => request.header(IF_NONE_MATCH, "*"),
        };
        Ok(request.send().await?.status())
    }

    pub async fn delete(&self, path: &str, expected_tag: &str) -> Result<()> {
        if expected_tag.is_empty() {
            bail!("cannot safely delete a remote file without an ETag");
        }
        let response = self
            .request(Method::DELETE, path)?
            .header(IF_MATCH, expected_tag)
            .send()
            .await?;
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

    async fn send_read_with_retry(
        &self,
        method: Method,
        path: &str,
        depth: Option<&str>,
        expected_tag: Option<&str>,
    ) -> Result<Response> {
        for attempt in 0..3 {
            let mut request = self.request(method.clone(), path)?;
            if let Some(depth) = depth {
                request = request.header("Depth", depth);
            }
            if let Some(tag) = expected_tag.filter(|tag| !tag.is_empty()) {
                request = request.header(IF_MATCH, tag);
            }
            match request.send().await {
                Ok(response) if retryable_status(response.status()) && attempt < 2 => {
                    let delay = retry_delay(&response, attempt);
                    tokio::time::sleep(delay).await;
                }
                Ok(response) => return Ok(response),
                Err(error) if attempt < 2 && (error.is_connect() || error.is_timeout()) => {
                    tokio::time::sleep(Duration::from_millis(250 * (attempt as u64 + 1))).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        unreachable!()
    }
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn retry_delay(response: &Response, attempt: u32) -> Duration {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_millis(250 * u64::from(attempt + 1)))
}

pub fn validate_remote_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.ends_with('/')
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == ".." || part.contains('\\'))
    {
        bail!("unsafe remote path: {path}");
    }
    Ok(())
}

fn relative_to_root(path: &str, root: &str) -> Result<String> {
    if path == root {
        return Ok(String::new());
    }
    let prefix = format!("{root}/");
    path.strip_prefix(&prefix)
        .map(str::to_string)
        .with_context(|| format!("WebDAV returned {path} outside mapped root {root}"))
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
                b"href" => {
                    let text = reader.read_text(e.name())?;
                    href = Some(quick_xml::escape::unescape(&text)?.into_owned());
                }
                b"getetag" => {
                    let text = reader.read_text(e.name())?;
                    tag = Some(quick_xml::escape::unescape(&text)?.into_owned());
                }
                b"getcontentlength" => {}
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
                        .with_context(|| {
                            format!("WebDAV href {decoded} is outside the configured endpoint")
                        })?
                        .trim_matches('/')
                        .to_string();
                    if !path.is_empty() {
                        validate_remote_path(&path)?;
                    }
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

    #[test]
    fn remote_root_prefix_requires_a_path_boundary() {
        assert_eq!(relative_to_root("foo/file", "foo").unwrap(), "file");
        assert!(relative_to_root("foobar/file", "foo").is_err());
    }

    #[test]
    fn rejects_remote_parent_components() {
        assert!(validate_remote_path("folder/../outside").is_err());
        assert!(validate_remote_path("folder/file").is_ok());
    }

    #[test]
    fn root_listing_accepts_the_endpoint_response() {
        let base = Url::parse("https://example.com/dav/Home/").unwrap();
        let body = br#"<multistatus xmlns="DAV:"><response><href>/dav/Home/</href><propstat><prop><resourcetype><collection/></resourcetype></prop></propstat></response></multistatus>"#;

        let entries = parse_multistatus(body, &base).unwrap();

        assert_eq!(entries[0].0, "");
        assert!(entries[0].1.is_dir);
    }

    #[test]
    fn request_url_encodes_filename_delimiters_as_path_characters() {
        let client = WebDavClient::new("https://example.com/dav/Home", "user", "password").unwrap();

        let url = client.url("Documents/Media/Your #1 question?.pdf").unwrap();

        assert_eq!(
            url.as_str(),
            "https://example.com/dav/Home/Documents/Media/Your%20%231%20question%3F.pdf"
        );
        assert!(url.query().is_none());
        assert!(url.fragment().is_none());
    }

    #[test]
    fn multistatus_unescapes_xml_entities_in_remote_paths() {
        let base = Url::parse("https://example.com/dav/Home/").unwrap();
        let body = br#"<?xml version="1.0"?>
            <d:multistatus xmlns:d="DAV:">
                <d:response>
                    <d:href>/dav/Home/Pictures/NASA%E2%80%99s_SLS_&amp;_Falcon_9.jpg</d:href>
                    <d:propstat><d:prop><d:getetag>&quot;picture-tag&quot;</d:getetag><d:getcontentlength>1234</d:getcontentlength></d:prop></d:propstat>
                </d:response>
                <d:response>
                    <d:href>/dav/Home/Documents/Data%20Engineering%20&amp;%20Science%20by%20O'Reilly.md</d:href>
                </d:response>
            </d:multistatus>"#;

        let entries = parse_multistatus(body, &base).unwrap();

        assert_eq!(entries[0].0, "Pictures/NASA’s_SLS_&_Falcon_9.jpg");
        assert_eq!(entries[0].1.tag, "\"picture-tag\"");
        assert_eq!(
            entries[1].0,
            "Documents/Data Engineering & Science by O'Reilly.md"
        );
    }
}
