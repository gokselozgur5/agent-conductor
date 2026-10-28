use crate::{Store, StoreError};
use serde::Serialize;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct WorkflowDependency {
    pub predecessor_task_id: String,
    pub accepted_source: Option<String>,
}
impl Store {
    /// Immutable admitted plan, including its original project/artifact binding.
    /// # Errors
    /// Returns database errors.
    pub async fn workflow_plan(&self, task_id: &str) -> Result<Option<String>, StoreError> {
        Ok(
            sqlx::query_scalar("SELECT plan_json FROM workflow_dispatch WHERE task_id=?")
                .bind(task_id)
                .fetch_optional(&self.inner.readers)
                .await?,
        )
    }
    /// Reads durable dependency identities; null sources have not been admitted.
    /// # Errors
    /// Returns database errors.
    pub async fn workflow_dependencies(
        &self,
        task_id: &str,
    ) -> Result<Vec<WorkflowDependency>, StoreError> {
        Ok(sqlx::query_as("SELECT predecessor_task_id,accepted_source FROM workflow_dependencies WHERE task_id=? ORDER BY predecessor_task_id")
            .bind(task_id).fetch_all(&self.inner.readers).await?)
    }
}

pub(super) async fn validate_admission(
    writer: &mut sqlx::SqliteConnection,
    task_id: &str,
    input: &serde_json::Value,
) -> Result<Vec<String>, StoreError> {
    let dependencies: Vec<String> = match input.get("depends_on") {
        None => vec![],
        Some(value) => {
            serde_json::from_value(value.clone()).map_err(|_| invalid("invalid dependency list"))?
        }
    };
    if dependencies.len() > 16
        || dependencies
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != dependencies.len()
        || dependencies
            .iter()
            .any(|id| id.is_empty() || id.len() > 128 || id == task_id)
    {
        return Err(invalid("invalid dependency bounds"));
    }
    if let Some(value) = input.get("input_task").filter(|v| !v.is_null())
        && !value
            .as_str()
            .is_some_and(|id| dependencies.iter().any(|d| d == id))
    {
        return Err(invalid("input task must be a declared dependency"));
    }
    for predecessor in &dependencies {
        let project: Option<String> = sqlx::query_scalar(
            "SELECT json_extract(plan_json,'$.project') FROM workflow_dispatch WHERE task_id=?",
        )
        .bind(predecessor)
        .fetch_optional(&mut *writer)
        .await?
        .flatten();
        if project.as_deref().is_none()
            || project.as_deref() != input.get("project").and_then(serde_json::Value::as_str)
        {
            return Err(invalid(
                "dependency requires an earlier plan in the same project",
            ));
        }
    }
    Ok(dependencies)
}

/// Reads eligibility without consuming a wake or freezing an unclaimed plan.
pub(super) async fn eligible_sources(
    writer: &mut sqlx::SqliteConnection,
    task_id: &str,
) -> Result<Option<Vec<(String, String)>>, StoreError> {
    let dependencies: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT d.predecessor_task_id,d.accepted_source,CASE WHEN t.state='completed' AND json_extract(t.aggregate_json,'$.verification.verdict.accepted')=1 AND json_extract(t.aggregate_json,'$.verification.verdict.source')=json_extract(t.aggregate_json,'$.verification.source') THEN json_extract(t.aggregate_json,'$.verification.source') ELSE NULL END FROM workflow_dependencies d LEFT JOIN tasks t ON t.id=d.predecessor_task_id WHERE d.task_id=? ORDER BY d.predecessor_task_id")
        .bind(task_id).fetch_all(&mut *writer).await?;
    let mut sources = Vec::with_capacity(dependencies.len());
    for (id, frozen, current) in dependencies {
        let Some(source) = current else {
            return Ok(None);
        };
        if source.is_empty()
            || source.len() > 128
            || frozen.as_ref().is_some_and(|old| old != &source)
        {
            return Ok(None);
        }
        sources.push((id, source));
    }
    Ok(Some(sources))
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn dependencies_require_earlier_same_project_plans_and_rollback() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("deps.sqlite3")).await.unwrap();
        store
            .enqueue_workflow("first", r#"{"project":"/project"}"#, 0)
            .await
            .unwrap();
        for plan in [
            r#"{"project":"/project","depends_on":["missing"]}"#,
            r#"{"project":"/project","depends_on":["next"]}"#,
            r#"{"project":"/project","depends_on":["first","first"]}"#,
            r#"{"project":"/other","depends_on":["first"]}"#,
            r#"{"project":"/project","input_task":"first"}"#,
        ] {
            assert!(store.enqueue_workflow("next", plan, 1).await.is_err());
            assert!(store.workflow_plan("next").await.unwrap().is_none());
        }
        let plan = r#"{"project":"/project","depends_on":["first"],"input_task":"first"}"#;
        store.enqueue_workflow("next", plan, 1).await.unwrap();
        store.enqueue_workflow("next", plan, 2).await.unwrap();
        assert_eq!(store.workflow_dependencies("next").await.unwrap().len(), 1);
        // Existing plans are immutable, so a later edge cannot create a cycle.
        assert!(
            store
                .enqueue_workflow(
                    "first",
                    r#"{"project":"/project","depends_on":["next"]}"#,
                    3
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn only_completed_acceptance_admits_once_with_atomic_source_binding() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("deps.sqlite3")).await.unwrap();
        let boot = store.start_runtime(0).await.unwrap().boot_generation;
        store
            .enqueue_workflow("first", r#"{"project":"/project"}"#, 1)
            .await
            .unwrap();
        store
            .enqueue_workflow(
                "next",
                r#"{"project":"/project","depends_on":["first"]}"#,
                2,
            )
            .await
            .unwrap();
        store
            .enqueue_workflow("unrelated", r#"{}"#, 3)
            .await
            .unwrap();
        assert_eq!(
            store
                .claim_workflow(boot, 4)
                .await
                .unwrap()
                .unwrap()
                .task_id,
            "first"
        );
        store
            .finish_workflow("first", boot, r#"{"completed":true}"#, 5)
            .await
            .unwrap();
        assert_eq!(
            store
                .claim_workflow(boot, 6)
                .await
                .unwrap()
                .unwrap()
                .task_id,
            "unrelated"
        );
        store
            .finish_workflow("unrelated", boot, "{}", 7)
            .await
            .unwrap();
        assert!(
            store.claim_workflow(boot, 8).await.unwrap().is_none(),
            "dispatch receipt alone is not acceptance"
        );
        {
            let mut db = store.inner.writer.lock().await;
            sqlx::query("INSERT INTO projects VALUES('project','/project','fixture',NULL,0)")
                .execute(&mut *db)
                .await
                .unwrap();
            sqlx::query("INSERT INTO tasks(id,project_id,objective,state,authority_json,aggregate_json,created_at_ms,updated_at_ms) VALUES('first','project','fixture','verifying','{}','{\"verification\":{\"source\":\"snapshot:accepted\",\"verdict\":{\"accepted\":true,\"source\":\"snapshot:accepted\"}}}',0,0)").execute(&mut *db).await.unwrap();
        }
        assert!(store.claim_workflow(boot, 9).await.unwrap().is_none());
        {
            let mut db = store.inner.writer.lock().await;
            sqlx::query("UPDATE tasks SET state='completed'")
                .execute(&mut *db)
                .await
                .unwrap();
            sqlx::query("CREATE TRIGGER refuse_claim BEFORE UPDATE OF status ON workflow_dispatch WHEN NEW.status='running' BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&mut *db).await.unwrap();
        }
        assert!(store.claim_workflow(boot, 10).await.is_err());
        assert!(
            store.workflow_dependencies("next").await.unwrap()[0]
                .accepted_source
                .is_none()
        );
        {
            let mut db = store.inner.writer.lock().await;
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM audit_events WHERE kind='workflow.dependencies_admitted'",
            )
            .fetch_one(&mut *db)
            .await
            .unwrap();
            assert_eq!(count, 0);
            sqlx::query("DROP TRIGGER refuse_claim")
                .execute(&mut *db)
                .await
                .unwrap();
        }
        let (a, b) = tokio::join!(
            store.claim_workflow(boot, 11),
            store.claim_workflow(boot, 11)
        );
        assert_eq!(
            usize::from(a.unwrap().is_some()) + usize::from(b.unwrap().is_some()),
            1
        );
        assert_eq!(
            store.workflow_dependencies("next").await.unwrap()[0]
                .accepted_source
                .as_deref(),
            Some("snapshot:accepted")
        );
        let reopened = Store::open(root.path().join("deps.sqlite3")).await.unwrap();
        assert_eq!(
            reopened.workflow_dependencies("next").await.unwrap()[0]
                .accepted_source
                .as_deref(),
            Some("snapshot:accepted")
        );
    }
}
