use anyhow::{Context, Result};
use libsql::{params, Connection, Database};
use std::{
    collections::BTreeMap,
    fs::Metadata,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};
use uuid::Uuid;
use walkdir::WalkDir;

pub struct LocalIndex {
    _database: Database,
    connection: Connection,
}

impl LocalIndex {
    pub async fn open(path: PathBuf) -> Result<Self> {
        let database = libsql::Builder::new_local(path).build().await?;
        let connection = database.connect()?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 CREATE TABLE IF NOT EXISTS local_hashes (
                    mapping_id TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    modified_ns INTEGER NOT NULL,
                    blake3_hash TEXT NOT NULL,
                    PRIMARY KEY (mapping_id, relative_path)
                 );",
            )
            .await?;
        Ok(Self {
            _database: database,
            connection,
        })
    }

    pub async fn scan(&self, mapping_id: Uuid, root: &Path) -> Result<BTreeMap<String, String>> {
        let mapping_id = mapping_id.to_string();
        let mut files = BTreeMap::new();
        for entry in WalkDir::new(root).follow_links(false) {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            let (size, modified_ns) = file_signature(&entry.metadata()?)?;
            let hash = match self
                .cached_hash(&mapping_id, &relative, size, modified_ns)
                .await?
            {
                Some(hash) => hash,
                None => {
                    let hash = blake3::hash(&std::fs::read(entry.path())?)
                        .to_hex()
                        .to_string();
                    self.store_hash(&mapping_id, &relative, size, modified_ns, &hash)
                        .await?;
                    hash
                }
            };
            files.insert(relative, hash);
        }
        Ok(files)
    }

    async fn cached_hash(
        &self,
        mapping_id: &str,
        relative: &str,
        size: i64,
        modified_ns: i64,
    ) -> Result<Option<String>> {
        let mut rows = self
            .connection
            .query(
                "SELECT blake3_hash FROM local_hashes
                 WHERE mapping_id = ?1 AND relative_path = ?2 AND size = ?3 AND modified_ns = ?4",
                params![mapping_id, relative, size, modified_ns],
            )
            .await?;
        Ok(match rows.next().await? {
            Some(row) => Some(row.get(0)?),
            None => None,
        })
    }

    async fn store_hash(
        &self,
        mapping_id: &str,
        relative: &str,
        size: i64,
        modified_ns: i64,
        hash: &str,
    ) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO local_hashes (mapping_id, relative_path, size, modified_ns, blake3_hash)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(mapping_id, relative_path) DO UPDATE SET
                    size = excluded.size,
                    modified_ns = excluded.modified_ns,
                    blake3_hash = excluded.blake3_hash",
                params![mapping_id, relative, size, modified_ns, hash],
            )
            .await?;
        Ok(())
    }
}

fn file_signature(metadata: &Metadata) -> Result<(i64, i64)> {
    let size = i64::try_from(metadata.len()).context("file is too large to index")?;
    let modified = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .context("file modification time is before 1970")?;
    let modified_ns = i64::try_from(modified.as_nanos()).context("file timestamp is too large")?;
    Ok((size, modified_ns))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stores_and_reuses_a_local_hash() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("files");
        std::fs::create_dir(&root).unwrap();
        let file = root.join("note.txt");
        std::fs::write(&file, b"cached content").unwrap();
        let index = LocalIndex::open(directory.path().join("index.db"))
            .await
            .unwrap();
        let mapping_id = Uuid::new_v4();

        let files = index.scan(mapping_id, &root).await.unwrap();
        let expected = blake3::hash(b"cached content").to_hex().to_string();
        assert_eq!(files.get("note.txt"), Some(&expected));

        let (size, modified_ns) = file_signature(&std::fs::metadata(file).unwrap()).unwrap();
        let cached = index
            .cached_hash(&mapping_id.to_string(), "note.txt", size, modified_ns)
            .await
            .unwrap();
        assert_eq!(cached, Some(expected));
    }
}
