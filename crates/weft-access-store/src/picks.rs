//! The connection each of a program's access nodes uses on this install
//! (`weft_core::picks`): picked here, never written in the source.

use serde::{Deserialize, Serialize};

use weft_task_store::db::Client;

use crate::flows::connection_handle;
use crate::AccessError;

/// One pick to keep: the author's connection `grant_id` for `field` of the
/// node at `node` (its place, spelled), a field connecting to `service`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PickWrite {
    pub node: String,
    pub field: String,
    pub grant_id: uuid::Uuid,
    pub service: String,
}

/// Keep `writes` and forget `clears` (node, field) for `project_id`, all
/// or none, on `db` (the caller's transaction). A pick must name one of
/// the author's own connections (never an instance's: that would spend its
/// account on everybody's runs) to the field's service, or the whole
/// change is refused.
pub async fn change_install_picks(
    db: &impl Client,
    tenant: &str,
    project_id: uuid::Uuid,
    writes: &[PickWrite],
    clears: &[(String, String)],
) -> anyhow::Result<()> {
    for write in writes {
        let owned: Option<String> = db
            .scalar_opt(
                "SELECT service FROM access_grant WHERE id = $1 AND tenant_id = $2 AND instance_id IS NULL",
                &[&write.grant_id, &tenant],
            )
            .await?;
        let Some(service) = owned else {
            return Err(AccessError::Invalid(format!(
                "connection {} is not one of your connections on this install (`weft connect --list` shows them)",
                write.grant_id
            ))
            .into());
        };
        if service != write.service {
            return Err(AccessError::Invalid(format!(
                "'{}.{}' connects to '{}', and connection {} is a '{service}' one",
                write.node, write.field, write.service, write.grant_id
            ))
            .into());
        }
    }
    for write in writes {
        db.execute(
            "INSERT INTO install_pick (tenant_id, project_id, node, field, grant_id)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (project_id, node, field) DO UPDATE
               SET grant_id = EXCLUDED.grant_id, tenant_id = EXCLUDED.tenant_id, set_at = now()",
            &[&tenant, &project_id, &write.node, &write.field, &write.grant_id],
        )
        .await?;
    }
    for (node, field) in clears {
        db.execute(
            "DELETE FROM install_pick WHERE tenant_id = $1 AND project_id = $2 AND node = $3 AND field = $4",
            &[&tenant, &project_id, node, field],
        )
        .await?;
    }
    Ok(())
}

/// Every pick this install keeps for `project_id`, by node and field, each
/// as the handle a connection field holds (`{id, identity}`), its identity
/// read from the connection now.
pub async fn install_picks(db: &impl Client, tenant: &str, project_id: uuid::Uuid) -> anyhow::Result<weft_core::picks::Picks> {
    let rows: Vec<(String, String, uuid::Uuid, Option<String>)> = db
        .all(
            "SELECT p.node, p.field, p.grant_id, g.identity FROM install_pick p
             JOIN access_grant g ON g.id = p.grant_id
             WHERE p.tenant_id = $1 AND p.project_id = $2",
            &[&tenant, &project_id],
        )
        .await?;
    let mut picks = weft_core::picks::Picks::new();
    for (node, field, grant_id, identity) in rows {
        picks.entry(node).or_default().insert(field, connection_handle(grant_id, identity.as_deref()));
    }
    Ok(picks)
}

/// Everything the install keeps at a place of `project_id`, one row per
/// field: the picks (no instance) and every instance's values, each
/// connection with its service. What an activation holds against the
/// program (`weft_core::picks::activation_picks`) and a move against the
/// place it leaves (`weft_core::picks::check_move`).
pub async fn stored_fields(
    db: &impl Client,
    tenant: &str,
    project_id: uuid::Uuid,
) -> anyhow::Result<Vec<weft_core::picks::StoredField>> {
    let rows: Vec<(String, String, Option<String>, Option<String>)> = db
        .all(
            "SELECT p.node, p.field, g.service, NULL::TEXT FROM install_pick p
             JOIN access_grant g ON g.id = p.grant_id
             WHERE p.tenant_id = $1 AND p.project_id = $2
             UNION ALL
             SELECT v.node, v.field, g.service, v.instance_id FROM instance_value v
             LEFT JOIN access_grant g ON g.id = v.grant_id
             WHERE v.tenant_id = $1 AND v.project_id = $2",
            &[&tenant, &project_id],
        )
        .await?;
    rows.into_iter()
        .map(|(node, field, service, instance)| {
            let instance = instance
                .map(weft_core::instance::InstanceId::new)
                .transpose()
                .map_err(|e| anyhow::anyhow!("a stored instance id is invalid: {e}"))?;
            Ok(weft_core::picks::StoredField { node, field, service, instance })
        })
        .collect()
}

/// Carry every pick and every instance's value kept at the place `from`
/// of `project_id` to the place `to`, on `db` (the caller's
/// transaction, which checked the move first with
/// `weft_core::picks::check_move`). Answers how many picks and how many
/// instance values moved.
pub async fn move_stored(
    db: &impl Client,
    tenant: &str,
    project_id: uuid::Uuid,
    from: &str,
    to: &str,
) -> anyhow::Result<(u64, u64)> {
    let picks = db
        .execute(
            "UPDATE install_pick SET node = $4, set_at = now() WHERE tenant_id = $1 AND project_id = $2 AND node = $3",
            &[&tenant, &project_id, &from, &to],
        )
        .await?;
    let values = db
        .execute(
            "UPDATE instance_value SET node = $4, set_at = now() WHERE tenant_id = $1 AND project_id = $2 AND node = $3",
            &[&tenant, &project_id, &from, &to],
        )
        .await?;
    Ok((picks, values))
}
