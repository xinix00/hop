//! De volumes van een jobspec in START_SLOT (sinds de ABI van HopOS alpha.11).
//!
//! De jobspec zegt `"volumes": {gedeeld pad: pad in de taak}` (Go's
//! `Job.Volumes`, hostpad naar taakpad); [`StartSpec::mounts`] houdt die
//! richting. Op de draad staat per volume eerst het lokale pad en dan het
//! gedeelde ([`systemapi::mount_blob`]). De kern normaliseert en weigert
//! wat de toegangsgrens raakt (`/`, `/.tasks`, `..`); hier alleen de vorm,
//! zodat een te grote jobspec een weigering is vóór de claim.

use abi::systemapi::{self, MountRef, StartReq};
use alloc::vec::Vec;
use runner::{StartSpec, SysError};

fn refused(reason: &str) -> SysError {
    SysError::Refused(alloc::format!("start mounts: {reason}"))
}

/// De volume-blob van `spec`, in de draadvorm van abi.
pub(super) fn blob(spec: &StartSpec) -> Result<Vec<u8>, SysError> {
    if spec.mounts.len() > systemapi::MAX_START_MOUNTS {
        return Err(refused("too many volumes"));
    }
    let mut refs = Vec::new();
    refs.try_reserve_exact(spec.mounts.len())
        .map_err(|_| refused("out of memory"))?;
    let mut bytes = 0;
    for (shared, local) in &spec.mounts {
        if shared.is_empty()
            || local.is_empty()
            || shared.len() > systemapi::MAX_MOUNT_PATH
            || local.len() > systemapi::MAX_MOUNT_PATH
        {
            return Err(refused("invalid path length"));
        }
        bytes += 4 + shared.len() + local.len();
        refs.push(MountRef {
            local: local.as_bytes(),
            shared: shared.as_bytes(),
        });
    }
    let mut out = Vec::new();
    out.try_reserve_exact(bytes)
        .map_err(|_| refused("out of memory"))?;
    out.resize(bytes, 0);
    systemapi::mount_blob(&refs, &mut out)
        .map_err(|e| SysError::Refused(alloc::format!("start mounts: {e:?}")))?;
    Ok(out)
}
/// Hangt de blob aan de start.
pub(super) fn attach<'a>(req: &mut StartReq<'a>, mounts: &'a [u8]) {
    req.mounts = mounts;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_to_app_mapping_and_bounds() {
        let mut spec = crate::tests::spec(1);
        spec.mounts.insert("/volumes/media".into(), "/media".into());
        let bytes = blob(&spec).unwrap();
        let mut req = StartReq::default();
        attach(&mut req, &bytes);
        let mounts: Vec<_> = systemapi::Mounts::new(req.mounts)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            mounts,
            [MountRef {
                local: b"/media",
                shared: b"/volumes/media"
            }]
        );
        for i in 0..systemapi::MAX_START_MOUNTS {
            spec.mounts
                .insert(alloc::format!("/volume/{i}"), alloc::format!("/app/{i}"));
        }
        assert!(blob(&spec).is_err());
    }
}
