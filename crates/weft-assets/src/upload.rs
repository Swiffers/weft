//! The one upload protocol into a tenant's assets, whoever drives it.
//!
//! An asset is stored under its content hash through the store's multipart
//! contract: begin (the store mints the key and says whether the content is
//! already stored or part way up), resume, reserve each part by number, PUT
//! its bytes straight to the presigned URL, report the part, complete. Two
//! planes speak it: a person's machine (the CLI, through the dispatcher's
//! `/storage/*` routes) and the install itself (the dispatcher, through the
//! broker's admin surface). [`UploadPlane`] is what differs between them;
//! [`ContentStore`] is the protocol, once, and the [`AssetStore`] every
//! publisher ([`crate::publish_hashed`]) uploads through.
//!
//! The store holds every completed asset to its address: bytes that do not
//! hash to the key's id are refused at complete. So the bytes are streamed
//! straight from the source, hashed while they are sent, and an upload whose
//! bytes turned out not to be the content it was begun for is aborted here
//! before it is completed: the file changed between its hashing and its
//! upload.

use std::collections::BTreeMap;
use std::io::Read;

use anyhow::{bail, Context, Result};
use sha2::Digest;
use weft_core::storage::{
    AssetUploadBeginRequest, AssetsHeldResponse, PartAsk, PartDoneRequest, PresignedPart, StoredFileMeta,
    UploadBeginResponse, UploadResumeResponse,
};

use crate::AssetStore;

/// The control calls of one tenant's asset uploads, as one caller reaches
/// them. The bytes never pass through it: each part is PUT straight to the
/// URL [`Self::parts`] or [`Self::resume`] presigned.
#[async_trait::async_trait]
pub trait UploadPlane: Send + Sync {
    /// Which of `hashes` the tenant stores whole, and the key each other
    /// one would be stored under.
    async fn held(&self, hashes: &[String]) -> Result<AssetsHeldResponse>;
    /// Begin the upload of one content.
    async fn begin(&self, request: AssetUploadBeginRequest) -> Result<UploadBeginResponse>;
    /// Pick up the upload already part way up under `key`.
    async fn resume(&self, key: &str) -> Result<UploadResumeResponse>;
    /// Reserve and presign `parts` of the upload under `key`.
    async fn parts(&self, key: &str, parts: Vec<PartAsk>) -> Result<Vec<PresignedPart>>;
    /// Record a landed part's etag.
    async fn part_done(&self, request: PartDoneRequest) -> Result<()>;
    /// Finish the upload under `key`.
    async fn complete(&self, key: &str) -> Result<()>;
    /// End the upload under `key`, freeing its reservation.
    async fn abort(&self, key: &str) -> Result<()>;
    /// The stored file under `key`; `None` when there is none.
    async fn meta_if_found(&self, key: &str) -> Result<Option<StoredFileMeta>>;
    /// Whether `error` is the store saying another caller's completion of
    /// this upload has claimed it (`weft_core::storage::COMPLETING_HEADER`):
    /// the file is about to land.
    fn completing(&self, error: &anyhow::Error) -> bool;
    /// Whether `error` is a request cut off on the way (a dropped
    /// connection, a timeout) rather than an answer.
    fn interrupted(&self, error: &anyhow::Error) -> bool;
}

/// A tenant's content-addressed assets over an [`UploadPlane`].
pub struct ContentStore<P> {
    plane: P,
    /// For the presigned part PUTs, which go straight to the bucket.
    http: reqwest::Client,
    /// Says on stderr how far the uploads have got, when set.
    told: Option<std::sync::Mutex<Uploads>>,
}

/// How far a publish's uploads have got, as last said.
struct Uploads {
    total: usize,
    done: usize,
    said_at: std::time::Instant,
}

/// How often a publish says how far it has got: a first publish to a new
/// install sends every file of the project (its copy of the standard
/// library included), and the person waiting should see it moving.
const UPLOAD_TELL_EVERY: std::time::Duration = std::time::Duration::from_secs(3);

impl<P: UploadPlane> ContentStore<P> {
    pub fn new(plane: P) -> Self {
        Self { plane, http: reqwest::Client::new(), told: None }
    }

    /// [`Self::new`], saying on stderr how far its uploads have got.
    pub fn telling(plane: P) -> Self {
        let told = Uploads { total: 0, done: 0, said_at: std::time::Instant::now() };
        Self { told: Some(std::sync::Mutex::new(told)), ..Self::new(plane) }
    }

    /// The plane's whole answer to which of `hashes` the tenant stores,
    /// every key checked to be the asset its hash names. [`AssetStore::held`]
    /// is its `keys`; a caller that also needs the keys of what is not
    /// stored yet reads `unheld`.
    pub async fn held_answer(&self, hashes: &[String]) -> Result<AssetsHeldResponse> {
        let held = self.plane.held(hashes).await.context("ask which contents are already stored")?;
        // Each key must be the asset its hash names. Parsed through the one
        // key grammar and failed loud on anything else: an entry taken on
        // faith would skip uploading content the store does not hold.
        for (hash, key) in held.keys.iter().chain(&held.unheld) {
            let parsed = weft_core::storage::key::parse_key(key)
                .map_err(|e| anyhow::anyhow!("the stored-contents answer holds a malformed key: {e}"))?;
            if parsed.scope != weft_core::storage::key::KeyScope::Asset || &parsed.id != hash {
                bail!("the stored-contents answer names '{key}' for {hash}, which is not that content's asset");
            }
        }
        if let Some(told) = &self.told {
            let missing = hashes.len().saturating_sub(held.keys.len());
            if missing > 0 {
                eprintln!("uploading {missing} files the install does not have yet");
            }
            let mut told = told.lock().expect("upload progress");
            (told.total, told.done) = (missing, 0);
        }
        Ok(held)
    }

    /// One more upload landed: say so when it has been a while, or when it
    /// was the last.
    fn note_uploaded(&self) {
        let Some(told) = &self.told else { return };
        let mut told = told.lock().expect("upload progress");
        told.done += 1;
        if told.done == told.total || told.said_at.elapsed() >= UPLOAD_TELL_EVERY {
            told.said_at = std::time::Instant::now();
            eprintln!("uploaded {} of {} files the install did not have yet", told.done, told.total);
        }
    }

    /// PUT one reserved part's bytes and report its etag.
    async fn put_part(&self, key: &str, part: &PresignedPart, body: &[u8]) -> Result<()> {
        let resp = self.http.put(&part.url).body(body.to_vec()).send().await.context("PUT asset part to bucket")?;
        if !resp.status().is_success() {
            bail!("asset part PUT failed: HTTP {}", resp.status());
        }
        let etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .context("bucket returned no ETag for asset part")?
            .to_string();
        self.plane
            .part_done(PartDoneRequest { key: key.to_string(), part_number: part.part_number, etag })
            .await
            .context("report asset part")
    }

    /// Upload `bytes` as the content `hash`, `size_bytes` long, and answer
    /// its key: this call's upload, or another publisher's of the same
    /// content that finished first.
    async fn transfer(
        &self,
        hash: &str,
        mime: &str,
        filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> Result<String> {
        let mut upload_key = None;
        // Set when the bytes read are not the content: that upload is ended
        // here, so no other publisher's finish can answer for it.
        let mut changed = false;
        let mut bytes = Hashing { inner: bytes, hasher: sha2::Sha256::new() };
        let transferred: Result<String> = async {
            let begin = self
                .plane
                .begin(AssetUploadBeginRequest {
                    mime_type: mime.to_string(),
                    filename: filename.to_string(),
                    declared_size: Some(size_bytes),
                    content_hash: hash.to_string(),
                })
                .await
                .with_context(|| format!("begin the upload of {filename}"))?;
            // Three answers, all from the begin itself, so this never has to
            // guess the key: the store mints it (it is tenant-anchored) and
            // says it in every one of them.
            upload_key = Some(begin.key.clone());
            // Already stored under this content's key: the upload's
            // idempotent success. Content-addressed, so the same bytes are
            // the same asset, and the begin answers the existing key with
            // nothing to transfer.
            if begin.already_stored {
                return Ok(begin.key);
            }
            let key = begin.key;
            let (part_size, mut reserved, carved) = if begin.resume {
                // The same content is already part way up: an earlier
                // publish that stopped (a Ctrl-C, a dropped connection), or
                // another publish of the same asset running right now.
                // Carrying on with it is safe either way, because a part is
                // reserved by NUMBER and both writers put identical bytes in
                // it. The first to call complete claims the upload; from
                // then on the store refuses the other's changes as
                // "completing", and that one waits for the file to land
                // (see the error path below).
                let resumed = self.plane.resume(&key).await.with_context(|| {
                    // When it was swept between the begin and this call, its
                    // key freed itself and the next attempt starts fresh.
                    format!(
                        "the unfinished upload of {filename} could not be picked up (it may have been cleared \
                         from the store meanwhile). Nothing is lost: run the same command again"
                    )
                })?;
                (resumed.part_size as usize, resumed.missing, resumed.reserved_bytes)
            } else {
                (begin.part_size as usize, Vec::new(), 0u64)
            };

            // Stream: read one part-sized chunk at a time, reserve + PUT + report.
            let mut buf = vec![0u8; part_size];
            // How far into the file the reader has got. The file is read once,
            // forwards, because it arrives as a stream and cannot be seeked.
            let mut at = 0u64;
            // The parts a resume named are already RESERVED, with their own
            // numbers, sizes and places in the file, so they are sent as they
            // are before anything new is reserved: `complete` refuses while a
            // reserved part has no etag, so skipping them would leave the
            // upload unfinishable.
            //
            // In offset order, and each one written at the offset the STORE
            // stated. A resume can name a part from the middle of the file
            // (any part whose PUT failed while a later one succeeded), and
            // working the offsets out here instead meant assuming the parts
            // handed back are one run at the end of what has landed.
            reserved.sort_by_key(|p| p.offset_bytes);
            // The sizes and offsets came off the wire, like the part URLs, so
            // they are checked before they index anything: a part claiming
            // more than one part's worth, or a place past the end of the
            // file, would have panicked on the slice rather than failed.
            if let Some(bad) = reserved.iter().find(|p| {
                p.size_bytes > part_size as u64 || p.offset_bytes.saturating_add(p.size_bytes) > size_bytes
            }) {
                bail!(
                    "the store placed part {} of the unfinished upload of {filename} at byte {} with \
                     {} bytes, which does not fit a {size_bytes} byte file in parts of up to \
                     {part_size} bytes; cancel that upload and publish again",
                    bad.part_number,
                    bad.offset_bytes,
                    bad.size_bytes
                );
            }
            // Bytes the store already holds are read and dropped, never
            // sent, and still hashed: the hash is of the whole file.
            let skip_to = |bytes: &mut Hashing<'_>, buf: &mut [u8], at: &mut u64, to: u64| -> Result<()> {
                while *at < to {
                    let want = (buf.len() as u64).min(to - *at) as usize;
                    if fill(bytes, &mut buf[..want]).context("skip already-uploaded bytes")? < want {
                        bail!(
                            "asset {filename} is shorter than the {to} bytes already uploaded under its own \
                             content hash; cancel that upload and publish again"
                        );
                    }
                    *at += want as u64;
                }
                Ok(())
            };
            for part in reserved {
                skip_to(&mut bytes, &mut buf, &mut at, part.offset_bytes)?;
                let want = part.size_bytes as usize;
                let filled = fill(&mut bytes, &mut buf[..want]).context("read asset chunk")?;
                if filled < want {
                    bail!(
                        "asset {filename} shrank while uploading (read {at} + {filled} of {size_bytes} \
                         bytes); run the command again"
                    );
                }
                self.put_part(&key, &part, &buf[..want]).await?;
                at += want as u64;
            }
            // New parts begin where the store stopped carving, which is not
            // where the last resent part ended whenever the missing part was
            // in the middle. Forward the reader there.
            skip_to(&mut bytes, &mut buf, &mut at, carved)?;
            while at < size_bytes {
                let want = part_size.min((size_bytes - at) as usize);
                let filled = fill(&mut bytes, &mut buf[..want]).context("read asset chunk")?;
                if filled < want {
                    bail!(
                        "asset {filename} shrank while uploading (read {at} + {filled} of {size_bytes} \
                         bytes); run the command again"
                    );
                }
                // The part NUMBER is the position, so this says which part of
                // the file it is about to send rather than asking for "the next
                // one". Two publishes of the same content (the key is its hash,
                // so the bytes are identical) then name the same part instead of
                // each extending the reservation past the end of the file.
                let part_number = (at / part_size as u64) as i32 + 1;
                let part = self
                    .plane
                    .parts(&key, vec![PartAsk { part_number, size_bytes: want as u64 }])
                    .await
                    .context("reserve asset part")?
                    .into_iter()
                    .next()
                    .context("the store reserved no part")?;
                self.put_part(&key, &part, &buf[..want]).await?;
                at += want as u64;
            }

            // Every byte sent was hashed on its way: bytes that are not the
            // content the upload was begun for would be refused by the
            // store's complete, so the upload is ended here instead.
            let sent = format!("{:x}", std::mem::take(&mut bytes.hasher).finalize());
            if sent != hash {
                changed = true;
                let said = format!(
                    "{filename} changed while it was being published (it hashed to {hash}, and the bytes \
                     uploaded hash to {sent}); run the command again"
                );
                return Err(match self.plane.abort(&key).await {
                    Ok(()) => anyhow::anyhow!(said),
                    Err(e) => e.context(format!("{said}. Ending its upload failed too")),
                });
            }
            self.plane.complete(&key).await.context("complete asset upload")?;
            Ok(key)
        }
        .await;
        let error = match transferred {
            Ok(uploaded) => return Ok(uploaded),
            Err(error) => error,
        };
        // The begin itself can find another publisher completing these same
        // bytes (a save the runtime syncs while a command publishes it):
        // there is no key yet, so the content is waited for by its hash.
        if upload_key.is_none() && !changed && self.plane.completing(&error) {
            return self.wait_for_content(hash, error).await;
        }
        // Any transfer step can lose to another publisher finishing these
        // same content-addressed bytes. When the store said so outright (a
        // "completing" refusal), the file is about to land: wait for it with
        // the same bounded backoff the engine gives a completion. Otherwise
        // look once, without retrying the upload or interpreting error text.
        let Some(key) = upload_key.filter(|_| !changed) else { return Err(error) };
        let completing = self.plane.completing(&error);
        let mut attempt = 0;
        loop {
            let stored = self
                .plane
                .meta_if_found(&key)
                .await
                .with_context(|| format!("{error:#}; could not determine whether another publisher completed this asset"))?;
            if let Some(meta) = stored {
                // The store holds every asset to its key's hash, so a file
                // under this key is this content; the size is checked to
                // fail loud on a store that broke that.
                anyhow::ensure!(
                    meta.key == key && meta.size_bytes == size_bytes,
                    "{error:#}; the store holds {} under {key} at {} bytes where {filename} is {size_bytes} bytes, \
                     so that upload is not this content",
                    meta.key,
                    meta.size_bytes
                );
                return Ok(key);
            }
            if !completing {
                return Err(error);
            }
            let Some(delay) = completing_wait(attempt) else {
                bail!(
                    "{error:#}; another publish of {filename} was completing it as {key}, and the file \
                     had not landed after waiting. The store finishes or ends that upload on its own; \
                     run the command again"
                );
            };
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }
}

impl<P: UploadPlane> ContentStore<P> {
    /// The key of `hash`'s content once another publisher's completion of
    /// it lands, with the bounded backoff a completion has everywhere
    /// ([`completing_wait`]); `error` (the refused begin) says why it was
    /// waited for if it never does.
    async fn wait_for_content(&self, hash: &str, error: anyhow::Error) -> Result<String> {
        let mut attempt = 0;
        loop {
            if let Some(key) = self.plane.held(&[hash.to_string()]).await.context("ask which contents are already stored")?.keys.remove(hash) {
                return Ok(key);
            }
            let Some(delay) = completing_wait(attempt) else {
                bail!(
                    "{error:#}; another publish was completing this content, and it had not landed after waiting. \
                     The store finishes or ends that upload on its own; run the command again"
                );
            };
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }
}

#[async_trait::async_trait]
impl<P: UploadPlane> AssetStore for ContentStore<P> {
    async fn held(&self, hashes: &[String]) -> Result<BTreeMap<String, String>> {
        Ok(self.held_answer(hashes).await?.keys)
    }

    async fn upload(
        &self,
        hash: &str,
        mime: &str,
        filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> Result<String> {
        // NOT wrapped in a Ctrl-C handler, and that is deliberate.
        //
        // Cancelling the upload on an interrupt looks right and costs too
        // much: `tokio::signal::ctrl_c` installs a PROCESS-GLOBAL handler
        // that permanently replaces the default die-on-SIGINT, and it is
        // not lifted when the future is dropped. An upload happens early
        // in `weft run` and `weft build`, long before the waits whose
        // documented recovery IS Ctrl-C (the infra wait, a build, a live
        // connection), so taking the signal here left those unkillable.
        //
        // The interrupted upload is handled where it lands instead: the
        // store keeps what arrived, and the next publish of the same
        // content resumes it through the upload protocol. Nothing is lost
        // and nothing is blocked, so there is no reason to own the signal.
        let key = self.transfer(hash, mime, filename, size_bytes, bytes).await?;
        self.note_uploaded();
        Ok(key)
    }

    fn interrupted(&self, error: &anyhow::Error) -> bool {
        self.plane.interrupted(error)
    }
}

/// A reader that hashes every byte read through it.
struct Hashing<'a> {
    inner: &'a mut (dyn Read + Send),
    hasher: sha2::Sha256,
}

impl Read for Hashing<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

/// Read into the whole of `buf`, or until the reader ends: how many bytes
/// were read.
fn fill(reader: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

/// How long to wait before looking again for a file another caller is
/// completing, after `attempt` looks found nothing; `None` once the wait is
/// over. 1s doubling, capped at 30s, twelve times: about four minutes, near
/// the store's completion lease, after which its sweep finishes or ends the
/// upload itself.
// SYNC: completion backoff <-> crates/weft-engine/src/storage.rs COMPLETE_ATTEMPTS / complete_until_landed
fn completing_wait(attempt: u32) -> Option<std::time::Duration> {
    const ATTEMPTS: u32 = 12;
    (attempt < ATTEMPTS).then(|| std::time::Duration::from_secs(1u64 << attempt.min(5)).min(std::time::Duration::from_secs(30)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// The bucket the part URLs point at: records every part PUT to it,
    /// and refuses them all when `refuse` is set.
    #[derive(Default)]
    struct Bucket {
        parts: Mutex<Vec<(i32, Vec<u8>)>>,
        refuse: std::sync::atomic::AtomicBool,
    }

    /// Serve `bucket` on a local port: `PUT /part/{n}` lands part `n`.
    async fn serve(bucket: Arc<Bucket>) -> (String, tokio::task::JoinHandle<()>) {
        use axum::response::IntoResponse;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().route(
            "/part/{n}",
            axum::routing::put(move |axum::extract::Path(n): axum::extract::Path<i32>, body: axum::body::Bytes| {
                let bucket = bucket.clone();
                async move {
                    if bucket.refuse.load(std::sync::atomic::Ordering::SeqCst) {
                        return axum::http::StatusCode::CONFLICT.into_response();
                    }
                    bucket.parts.lock().unwrap().push((n, body.to_vec()));
                    (axum::http::StatusCode::OK, [("etag", format!("etag-{n}"))], "").into_response()
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, server)
    }

    /// The store's "another caller is completing this upload".
    #[derive(Debug)]
    struct Completing;
    impl std::fmt::Display for Completing {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("completing")
        }
    }
    impl std::error::Error for Completing {}

    /// A hand-rolled store: the begin answer it is given, a step that
    /// fails, and the file it holds after a number of looks.
    struct FakePlane {
        base: String,
        begin: UploadBeginResponse,
        resume: Option<UploadResumeResponse>,
        /// The step that is refused: "resume", "parts", "part-done", "complete".
        refuse: Option<&'static str>,
        /// `complete` answers "completing" rather than a plain refusal.
        completing: bool,
        /// The looks at the file that find nothing before it is there;
        /// `None`, it never is.
        stored_after: Option<u32>,
        looks: Mutex<u32>,
        calls: Mutex<Vec<String>>,
    }

    impl FakePlane {
        fn new(base: &str, begin: UploadBeginResponse) -> Self {
            Self {
                base: base.to_string(),
                begin,
                resume: None,
                refuse: None,
                completing: false,
                stored_after: None,
                looks: Mutex::new(0),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn step(&self, name: &str) -> Result<()> {
            self.calls.lock().unwrap().push(name.to_string());
            match self.refuse {
                Some(refused) if refused == name && self.completing => Err(Completing.into()),
                Some(refused) if refused == name => bail!("the store refused {name}"),
                _ => Ok(()),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl UploadPlane for FakePlane {
        async fn held(&self, hashes: &[String]) -> Result<AssetsHeldResponse> {
            // Only a begin the other publish's completion refused asks.
            self.calls.lock().unwrap().push("held".into());
            let mut looks = self.looks.lock().unwrap();
            *looks += 1;
            let landed = self.stored_after.is_some_and(|after| *looks > after);
            Ok(AssetsHeldResponse {
                keys: hashes.iter().filter(|_| landed).map(|hash| (hash.clone(), "t/asset/k".to_string())).collect(),
                unheld: Default::default(),
            })
        }
        async fn begin(&self, _request: AssetUploadBeginRequest) -> Result<UploadBeginResponse> {
            self.step("begin")?;
            Ok(self.begin.clone())
        }
        async fn resume(&self, _key: &str) -> Result<UploadResumeResponse> {
            self.step("resume")?;
            Ok(self.resume.clone().expect("a resumed upload"))
        }
        async fn parts(&self, _key: &str, parts: Vec<PartAsk>) -> Result<Vec<PresignedPart>> {
            self.step("parts")?;
            let part_size = self.begin.part_size;
            Ok(parts
                .into_iter()
                .map(|ask| PresignedPart {
                    part_number: ask.part_number,
                    size_bytes: ask.size_bytes,
                    offset_bytes: (ask.part_number as u64 - 1) * part_size,
                    url: format!("{}/part/{}", self.base, ask.part_number),
                })
                .collect())
        }
        async fn part_done(&self, request: PartDoneRequest) -> Result<()> {
            assert_eq!(request.etag, format!("etag-{}", request.part_number));
            self.step("part-done")
        }
        async fn complete(&self, _key: &str) -> Result<()> {
            self.step("complete")
        }
        async fn abort(&self, _key: &str) -> Result<()> {
            self.step("abort")
        }
        async fn meta_if_found(&self, key: &str) -> Result<Option<StoredFileMeta>> {
            self.calls.lock().unwrap().push("meta".into());
            let mut looks = self.looks.lock().unwrap();
            *looks += 1;
            Ok(self.stored_after.filter(|after| *looks > *after).map(|_| {
                serde_json::from_value(serde_json::json!({"key": key, "mimeType": "text/plain", "sizeBytes": 4,
                    "filename": "test", "keep": false, "createdAtUnix": 0, "version": 1}))
                .unwrap()
            }))
        }
        fn completing(&self, error: &anyhow::Error) -> bool {
            error.chain().any(|cause| cause.is::<Completing>())
        }
        fn interrupted(&self, _error: &anyhow::Error) -> bool {
            false
        }
    }

    fn sha(bytes: &[u8]) -> String {
        crate::hash_reader(bytes).unwrap().0
    }

    fn fresh(part_size: u64) -> UploadBeginResponse {
        UploadBeginResponse { key: "t/asset/k".into(), part_size, already_stored: false, resume: false }
    }

    async fn upload(store: &ContentStore<FakePlane>, hash: &str, bytes: &[u8]) -> Result<String> {
        store.upload(hash, "text/plain", "test", bytes.len() as u64, &mut &bytes[..]).await
    }

    /// The wait for a file another caller is completing has the engine's
    /// shape: 1s doubling, capped at 30s, then over.
    #[test]
    fn the_completing_wait_doubles_caps_and_ends() {
        let waits: Vec<u64> = (0..).map_while(completing_wait).map(|d| d.as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30, 30, 30, 30, 30, 30]);
    }

    /// A new content goes up part by part, straight from the stream, and
    /// is completed.
    #[tokio::test]
    async fn a_new_content_streams_part_by_part_and_completes() {
        let bucket = Arc::new(Bucket::default());
        let (base, server) = serve(bucket.clone()).await;
        let plane = FakePlane::new(&base, fresh(4));
        let store = ContentStore::new(plane);
        let key = upload(&store, &sha(b"abcdefghij"), b"abcdefghij").await;
        server.abort();
        assert_eq!(key.unwrap(), "t/asset/k");
        assert_eq!(*bucket.parts.lock().unwrap(), [(1, b"abcd".to_vec()), (2, b"efgh".to_vec()), (3, b"ij".to_vec())]);
        assert_eq!(store.plane.calls(), ["begin", "parts", "part-done", "parts", "part-done", "parts", "part-done", "complete"]);
    }

    /// Content already stored is answered by the begin, and nothing is read.
    #[tokio::test]
    async fn content_already_stored_reads_nothing() {
        let plane = FakePlane::new("http://unused", UploadBeginResponse { already_stored: true, ..fresh(0) });
        let mut never: &[u8] = &[];
        let store = ContentStore::new(plane);
        let key = store.upload(&sha(b"data"), "text/plain", "test", 4, &mut never).await;
        assert_eq!(key.unwrap(), "t/asset/k");
        assert_eq!(store.plane.calls(), ["begin"]);
    }

    /// A resumed upload sends the part the store says is missing (from
    /// the middle of the file, at the offset the store states), then new
    /// parts from where the store stopped carving. The bytes it skips are
    /// hashed too: the whole file is the content.
    #[tokio::test]
    async fn a_resume_sends_the_missing_part_then_the_rest() {
        let bucket = Arc::new(Bucket::default());
        let (base, server) = serve(bucket.clone()).await;
        let plane = FakePlane {
            resume: Some(UploadResumeResponse {
                part_size: 2,
                missing: vec![PresignedPart { part_number: 2, size_bytes: 2, offset_bytes: 2, url: format!("{base}/part/2") }],
                reserved_bytes: 4,
            }),
            ..FakePlane::new(&base, UploadBeginResponse { resume: true, ..fresh(2) })
        };
        let store = ContentStore::new(plane);
        let key = upload(&store, &sha(b"abcdef"), b"abcdef").await;
        server.abort();
        assert_eq!(key.unwrap(), "t/asset/k");
        assert_eq!(*bucket.parts.lock().unwrap(), [(2, b"cd".to_vec()), (3, b"ef".to_vec())]);
        assert_eq!(store.plane.calls(), ["begin", "resume", "part-done", "parts", "part-done", "complete"]);
    }

    /// Bytes that are not the content the upload was begun for (the file
    /// changed after it was hashed) are never completed: the upload is
    /// ended, and the error says to run the command again.
    #[tokio::test]
    async fn bytes_that_changed_since_their_hash_end_the_upload() {
        let bucket = Arc::new(Bucket::default());
        let (base, server) = serve(bucket.clone()).await;
        let plane = FakePlane { stored_after: Some(0), ..FakePlane::new(&base, fresh(4)) };
        let store = ContentStore::new(plane);
        let err = upload(&store, &sha(b"data"), b"dat2").await.unwrap_err();
        server.abort();
        let said = format!("{err:#}");
        assert!(said.contains("test changed while it was being published"), "{said}");
        assert!(said.contains(&sha(b"data")) && said.contains(&sha(b"dat2")), "{said}");
        assert!(said.contains("run the command again"), "{said}");
        assert_eq!(store.plane.calls(), ["begin", "parts", "part-done", "abort"], "no complete, and no look for another publish");
    }

    /// Any step after the begin can lose to another publisher finishing
    /// the same content: the file being there is this upload's success,
    /// and it not being there is the step's error.
    #[tokio::test]
    async fn another_publisher_can_finish_at_any_upload_step() {
        for completed_elsewhere in [true, false] {
            for refused in ["resume", "parts", "put", "part-done", "complete"] {
                let bucket = Arc::new(Bucket::default());
                bucket.refuse.store(refused == "put", std::sync::atomic::Ordering::SeqCst);
                let (base, server) = serve(bucket.clone()).await;
                let plane = FakePlane {
                    resume: Some(UploadResumeResponse { part_size: 4, missing: Vec::new(), reserved_bytes: 0 }),
                    refuse: Some(refused),
                    stored_after: completed_elsewhere.then_some(0),
                    ..FakePlane::new(&base, UploadBeginResponse { resume: refused == "resume", ..fresh(4) })
                };
                let store = ContentStore::new(plane);
                let result = upload(&store, &sha(b"data"), b"data").await;
                server.abort();
                if completed_elsewhere {
                    assert_eq!(result.unwrap(), "t/asset/k", "refused at {refused}");
                } else {
                    assert!(result.is_err(), "refused at {refused}, nothing stored, cannot succeed");
                }
                assert_eq!(store.plane.calls().last().map(String::as_str), Some("meta"), "refused at {refused}: one look");
                assert_eq!(store.plane.calls().iter().filter(|c| *c == "meta").count(), 1, "refused at {refused}");
            }
        }
    }

    /// A save the runtime syncs and a command that publishes it can begin
    /// the same new content together: the begin that comes second is
    /// refused as "completing", has no key yet, and waits for the content
    /// by its hash instead of failing.
    #[tokio::test]
    async fn a_begin_refused_as_completing_waits_for_the_content_to_land() {
        let plane = FakePlane {
            refuse: Some("begin"),
            completing: true,
            stored_after: Some(1),
            ..FakePlane::new("http://unused", fresh(4))
        };
        let store = ContentStore::new(plane);
        let key = upload(&store, &sha(b"data"), b"data").await;
        assert_eq!(key.unwrap(), "t/asset/k");
        assert_eq!(store.plane.calls().iter().filter(|c| *c == "held").count(), 2, "it looked again after waiting");
        assert!(!store.plane.calls().contains(&"parts".to_string()), "nothing was uploaded: the other publish's copy landed");
    }

    /// Two publishes of the same new content: the other one claimed the
    /// completion, so this one's complete is refused as "completing" and
    /// the file is not there yet on the first look. It waits and gets the
    /// file.
    #[tokio::test]
    async fn a_completing_refusal_waits_for_the_other_publish_to_land() {
        let bucket = Arc::new(Bucket::default());
        let (base, server) = serve(bucket.clone()).await;
        let plane = FakePlane {
            refuse: Some("complete"),
            completing: true,
            stored_after: Some(1),
            ..FakePlane::new(&base, fresh(4))
        };
        let store = ContentStore::new(plane);
        let key = upload(&store, &sha(b"data"), b"data").await;
        server.abort();
        assert_eq!(key.unwrap(), "t/asset/k");
        assert_eq!(store.plane.calls().iter().filter(|c| *c == "meta").count(), 2, "it looked again after waiting");
    }
}
