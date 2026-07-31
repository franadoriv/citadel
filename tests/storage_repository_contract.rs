//! Contract tests for the storage repository (, ).
//!
//! These assert the version-conflict and permission semantics that *any*
//! [`StorageRepository`] implementation must honor. Each scenario is written
//! against `&dyn StorageRepository` and is run twice:
//!
//! - always against [`InMemoryStorageRepository`] (the reference impl), and
//! - against a real Postgres backend when `DATABASE_URL` (or
//!   `CITADEL_TEST_DATABASE_URL`) is set, proving both backends behave
//!   identically. The Postgres run is skipped when neither variable is set, so
//!   `bash scripts/check.sh` stays green without a database.
//!
//! Run the Postgres side locally with:
//!
//! ```text
//! make db-up            # start a throwaway Postgres in Docker
//! DATABASE_URL=postgres://citadel:citadel@localhost:5432/citadel \
//!   cargo test --test storage_repository_contract
//! make db-down
//! ```

use citadel::error::ErrorCategory;
use citadel::repository::{InMemoryStorageRepository, StorageRepository};
use citadel::storage::{
    Accessor, AtomicBatchOperation, Collection, Key, ListQuery, ObjectId, Owner, Permissions,
    Precondition, ReadPermission, StorageIndexDefinition, StorageIndexField,
    StorageIndexMembership, StorageIndexName, StorageIndexQuery, StorageValue, UserId,
    WritePermission, WriteRequest,
};
use serde_json::json;

fn user(id: &str) -> UserId {
    UserId::new(id).expect("valid user id")
}

fn id(owner: Owner, collection: &str, key: &str) -> ObjectId {
    ObjectId::new(
        owner,
        Collection::new(collection).expect("collection"),
        Key::new(key).expect("key"),
    )
}

fn value(score: i64) -> StorageValue {
    StorageValue::new(json!({ "score": score })).expect("value")
}

fn score_index() -> StorageIndexDefinition {
    StorageIndexDefinition::new(
        StorageIndexName::new("profiles_by_score").expect("index name"),
        Collection::new("profiles").expect("collection"),
        None,
        vec![StorageIndexField::new("score").expect("field")],
    )
    .expect("index definition")
}

// --- Scenarios (backend-agnostic) -------------------------------------------

async fn scenario_owner_can_write_and_read_own_object(repo: &dyn StorageRepository) {
    let alice = user("alice");
    let object = id(Owner::user(alice.clone()), "saves", "slot-1");

    let request = WriteRequest::upsert(object.clone(), value(7), Permissions::owner_private());
    let written = repo
        .write(&Accessor::User(alice.clone()), request)
        .await
        .expect("owner can write own object");

    let read = repo
        .read(&Accessor::User(alice), &object)
        .await
        .expect("read ok")
        .expect("present");
    assert_eq!(read.version, written.version);
    assert_eq!(read.value.as_json(), &json!({ "score": 7 }));
}

async fn scenario_client_cannot_create_object_for_another_owner(repo: &dyn StorageRepository) {
    let object = id(Owner::user(user("bob")), "saves", "slot-1");

    // Alice attempts to create an object owned by Bob.
    let request = WriteRequest::upsert(object, value(1), Permissions::owner_private());
    let err = repo
        .write(&Accessor::User(user("alice")), request)
        .await
        .expect_err("cross-owner create is denied");
    assert_eq!(err.category(), ErrorCategory::Permission);
}

async fn scenario_client_cannot_create_system_object(repo: &dyn StorageRepository) {
    let object = id(Owner::System, "config", "global");
    let request = WriteRequest::upsert(object, value(1), Permissions::public_read());
    let err = repo
        .write(&Accessor::User(user("alice")), request)
        .await
        .expect_err("client cannot create system object");
    assert_eq!(err.category(), ErrorCategory::Permission);
}

async fn scenario_non_owner_cannot_overwrite_owner_private_object(repo: &dyn StorageRepository) {
    let alice = user("alice");
    let object = id(Owner::user(alice.clone()), "saves", "slot-1");

    repo.write(
        &Accessor::User(alice),
        WriteRequest::upsert(object.clone(), value(1), Permissions::owner_private()),
    )
    .await
    .expect("owner creates object");

    let err = repo
        .write(
            &Accessor::User(user("mallory")),
            WriteRequest::upsert(object, value(99), Permissions::owner_private()),
        )
        .await
        .expect_err("non-owner overwrite denied");
    assert_eq!(err.category(), ErrorCategory::Permission);
}

async fn scenario_owner_only_object_is_invisible_to_other_clients_and_public(
    repo: &dyn StorageRepository,
) {
    let alice = user("alice");
    let object = id(Owner::user(alice.clone()), "saves", "slot-1");
    repo.write(
        &Accessor::User(alice),
        WriteRequest::upsert(object.clone(), value(1), Permissions::owner_private()),
    )
    .await
    .expect("owner creates object");

    // Another authenticated user and a public caller both see nothing, with no
    // way to distinguish "absent" from "forbidden".
    assert!(
        repo.read(&Accessor::User(user("mallory")), &object)
            .await
            .expect("read ok")
            .is_none()
    );
    assert!(
        repo.read(&Accessor::Public, &object)
            .await
            .expect("read ok")
            .is_none()
    );
}

async fn scenario_public_read_object_is_visible_to_everyone(repo: &dyn StorageRepository) {
    let alice = user("alice");
    let object = id(Owner::user(alice.clone()), "profiles", "alice");
    repo.write(
        &Accessor::User(alice),
        WriteRequest::upsert(object.clone(), value(1), Permissions::public_read()),
    )
    .await
    .expect("owner creates public object");

    assert!(
        repo.read(&Accessor::User(user("bob")), &object)
            .await
            .expect("read ok")
            .is_some()
    );
    assert!(
        repo.read(&Accessor::Public, &object)
            .await
            .expect("read ok")
            .is_some()
    );
}

async fn scenario_runtime_only_object_is_hidden_from_clients_but_visible_to_runtime(
    repo: &dyn StorageRepository,
) {
    let object = id(Owner::System, "secrets", "api-key");
    repo.write(
        &Accessor::Runtime,
        WriteRequest::upsert(object.clone(), value(1), Permissions::runtime_only()),
    )
    .await
    .expect("runtime creates object");

    assert!(
        repo.read(&Accessor::Public, &object)
            .await
            .expect("read ok")
            .is_none()
    );
    assert!(
        repo.read(&Accessor::User(user("alice")), &object)
            .await
            .expect("read ok")
            .is_none()
    );
    assert!(
        repo.read(&Accessor::Runtime, &object)
            .await
            .expect("read ok")
            .is_some()
    );
}

async fn scenario_create_only_precondition_rejects_duplicate(repo: &dyn StorageRepository) {
    let object = id(Owner::System, "config", "global");

    repo.write(
        &Accessor::Runtime,
        WriteRequest::upsert(object.clone(), value(1), Permissions::runtime_only())
            .expecting(Precondition::MustNotExist),
    )
    .await
    .expect("first create");

    let err = repo
        .write(
            &Accessor::Runtime,
            WriteRequest::upsert(object, value(2), Permissions::runtime_only())
                .expecting(Precondition::MustNotExist),
        )
        .await
        .expect_err("duplicate create rejected");
    assert_eq!(err.category(), ErrorCategory::Conflict);
}

async fn scenario_optimistic_version_match_round_trip(repo: &dyn StorageRepository) {
    let object = id(Owner::System, "config", "global");

    let v1 = repo
        .write(
            &Accessor::Runtime,
            WriteRequest::upsert(object.clone(), value(1), Permissions::runtime_only()),
        )
        .await
        .expect("first write");

    // A matching precondition succeeds and yields a new version.
    let v2 = repo
        .write(
            &Accessor::Runtime,
            WriteRequest::upsert(object.clone(), value(2), Permissions::runtime_only())
                .expecting(Precondition::Match(v1.version.clone())),
        )
        .await
        .expect("matching version write");
    assert_ne!(v1.version, v2.version);

    // Reusing the now-stale v1 version conflicts.
    let err = repo
        .write(
            &Accessor::Runtime,
            WriteRequest::upsert(object, value(3), Permissions::runtime_only())
                .expecting(Precondition::Match(v1.version)),
        )
        .await
        .expect_err("stale version conflicts");
    assert_eq!(err.category(), ErrorCategory::Conflict);
}

async fn scenario_delete_with_matching_version_then_missing_conflicts(
    repo: &dyn StorageRepository,
) {
    let object = id(Owner::System, "config", "global");
    let written = repo
        .write(
            &Accessor::Runtime,
            WriteRequest::upsert(object.clone(), value(1), Permissions::runtime_only()),
        )
        .await
        .expect("write");

    repo.delete(
        &Accessor::Runtime,
        &object,
        Precondition::Match(written.version.clone()),
    )
    .await
    .expect("delete with matching version");

    // The object is gone; a versioned delete now fails the precondition.
    let err = repo
        .delete(
            &Accessor::Runtime,
            &object,
            Precondition::Match(written.version),
        )
        .await
        .expect_err("versioned delete of missing object conflicts");
    assert_eq!(err.category(), ErrorCategory::Conflict);
}

async fn scenario_idempotent_delete_of_absent_object(repo: &dyn StorageRepository) {
    let object = id(Owner::System, "config", "missing");
    repo.delete(&Accessor::Runtime, &object, Precondition::Any)
        .await
        .expect("delete of absent object is idempotent");
}

async fn scenario_list_rejects_zero_limit(repo: &dyn StorageRepository) {
    let query = ListQuery::across_owners(Collection::new("saves").expect("collection"), 0);
    let err = repo
        .list(&Accessor::Runtime, &query)
        .await
        .expect_err("zero limit rejected");
    assert_eq!(err.category(), ErrorCategory::Validation);
}

async fn scenario_list_paginates_with_cursor_and_filters_by_permission(
    repo: &dyn StorageRepository,
) {
    let alice = user("alice");
    let owner = Owner::user(alice.clone());

    // Three readable objects owned by Alice.
    for n in 0..3 {
        repo.write(
            &Accessor::User(alice.clone()),
            WriteRequest::upsert(
                id(owner.clone(), "saves", &format!("slot-{n}")),
                value(n),
                Permissions::owner_private(),
            ),
        )
        .await
        .expect("write owner object");
    }
    // One object in the same collection owned by someone else; Alice must not
    // see it.
    repo.write(
        &Accessor::User(user("bob")),
        WriteRequest::upsert(
            id(Owner::user(user("bob")), "saves", "slot-bob"),
            value(9),
            Permissions::owner_private(),
        ),
    )
    .await
    .expect("write other object");

    let collection = Collection::new("saves").expect("collection");

    // First page of two, scoped to Alice.
    let query = ListQuery::for_owner(owner.clone(), collection.clone(), 2);
    let page1 = repo
        .list(&Accessor::User(alice.clone()), &query)
        .await
        .expect("list page 1");
    assert_eq!(page1.items.len(), 2);
    let cursor = page1.next.clone().expect("more pages remain");

    // Second page picks up the remaining object and ends pagination.
    let query2 = ListQuery::for_owner(owner, collection, 2).after(cursor);
    let page2 = repo
        .list(&Accessor::User(alice), &query2)
        .await
        .expect("list page 2");
    assert_eq!(page2.items.len(), 1);
    assert!(page2.next.is_none());

    // Across both pages only Alice's three objects appear (never Bob's).
    for object in page1.items.iter().chain(page2.items.iter()) {
        assert!(matches!(&object.id.owner, Owner::User(u) if u.as_str() == "alice"));
    }
}

async fn scenario_list_collections_counts_every_object(repo: &dyn StorageRepository) {
    // Empty repository: no collections.
    assert!(
        repo.list_collections()
            .await
            .expect("scan empty")
            .is_empty()
    );

    let alice = user("alice");
    // Two objects in `saves` (one owner-private, one runtime-only) and one in
    // `configs`: the administrative scan counts all of them regardless of
    // permissions.
    repo.write(
        &Accessor::User(alice.clone()),
        WriteRequest::upsert(
            id(Owner::user(alice.clone()), "saves", "slot-1"),
            value(1),
            Permissions::owner_private(),
        ),
    )
    .await
    .expect("write owner object");
    repo.write(
        &Accessor::Runtime,
        WriteRequest::upsert(
            id(Owner::System, "saves", "seed"),
            value(2),
            Permissions::runtime_only(),
        ),
    )
    .await
    .expect("write runtime object");
    repo.write(
        &Accessor::Runtime,
        WriteRequest::upsert(
            id(Owner::System, "configs", "main"),
            value(3),
            Permissions::runtime_only(),
        ),
    )
    .await
    .expect("write config object");

    let collections = repo.list_collections().await.expect("scan");
    let summary: Vec<(String, u64)> = collections
        .iter()
        .map(|c| (c.collection.as_str().to_string(), c.objects))
        .collect();
    assert_eq!(
        summary,
        vec![("configs".to_string(), 1), ("saves".to_string(), 2)],
        "name-ordered, permission-blind counts"
    );
}

async fn scenario_index_query_filters_declared_json_fields_and_permissions(
    repo: &dyn StorageRepository,
) {
    let index = score_index();
    repo.install_index(&index).await.expect("install index");

    let alice = user("alice");
    let bob = user("bob");
    repo.write(
        &Accessor::User(alice.clone()),
        WriteRequest::upsert(
            id(Owner::user(alice.clone()), "profiles", "private-alice"),
            value(7),
            Permissions::owner_private(),
        ),
    )
    .await
    .expect("alice private object");
    repo.write(
        &Accessor::User(bob.clone()),
        WriteRequest::upsert(
            id(Owner::user(bob.clone()), "profiles", "public-bob"),
            value(7),
            Permissions::public_read(),
        ),
    )
    .await
    .expect("bob public object");
    repo.write(
        &Accessor::User(alice.clone()),
        WriteRequest::upsert(
            id(Owner::user(alice.clone()), "profiles", "other-score"),
            value(8),
            Permissions::public_read(),
        ),
    )
    .await
    .expect("other score object");

    let filters = json!({"score": 7});
    let filters = filters.as_object().expect("object filters");
    let query = StorageIndexQuery::from_json_filters(index.clone(), filters, 10).expect("query");

    let runtime = repo
        .query_index(&Accessor::Runtime, &query)
        .await
        .expect("runtime query");
    assert_eq!(runtime.len(), 2);
    let runtime_keys = runtime
        .iter()
        .map(|object| object.id.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(runtime_keys, vec!["private-alice", "public-bob"]);

    let alice_visible = repo
        .query_index(&Accessor::User(alice), &query)
        .await
        .expect("alice query");
    assert_eq!(alice_visible.len(), 2, "owner-private + public are visible");
    assert!(
        alice_visible
            .iter()
            .all(|object| object.id.key.as_str() != "other-score")
    );
}

async fn scenario_index_membership_exclusion_is_durable_and_atomic(repo: &dyn StorageRepository) {
    let index = score_index();
    repo.install_index(&index).await.expect("install index");
    let alice = user("alice");
    let object_id = id(Owner::user(alice.clone()), "profiles", "main");
    let rejected_id = id(Owner::user(alice.clone()), "profiles", "rejected");
    let invalid_candidates = [StorageIndexName::new("unrelated").expect("name")]
        .into_iter()
        .collect();
    let invalid_membership = StorageIndexMembership::include_all(invalid_candidates);
    let error = repo
        .write_indexed(
            &Accessor::User(alice.clone()),
            WriteRequest::upsert(rejected_id.clone(), value(7), Permissions::public_read()),
            Some(&invalid_membership),
        )
        .await
        .expect_err("mismatched callback decision must abort the write");
    assert_eq!(error.category(), ErrorCategory::Validation);
    assert!(
        repo.read(&Accessor::Runtime, &rejected_id)
            .await
            .expect("read")
            .is_none(),
        "a rejected projection decision must roll the base object back too"
    );
    let request = WriteRequest::upsert(object_id.clone(), value(7), Permissions::public_read());
    let candidates = [index.name().clone()].into_iter().collect();
    let excluded = StorageIndexMembership::new(candidates, Default::default())
        .expect("valid explicit exclusion");
    repo.write_indexed(&Accessor::User(alice.clone()), request, Some(&excluded))
        .await
        .expect("write remains durable while excluded from index");

    assert!(
        repo.read(&Accessor::User(alice), &object_id)
            .await
            .expect("read")
            .is_some(),
        "filter exclusion must not delete the storage object"
    );
    let filters = json!({"score": 7});
    let query = StorageIndexQuery::from_json_filters(
        index.clone(),
        filters.as_object().expect("object filters"),
        10,
    )
    .expect("query");
    assert!(
        repo.query_index(&Accessor::Runtime, &query)
            .await
            .expect("query")
            .is_empty(),
        "excluded object must not be observable through the index"
    );

    repo.write(
        &Accessor::Runtime,
        WriteRequest::upsert(object_id, value(7), Permissions::public_read()),
    )
    .await
    .expect("ordinary write restores default inclusion");
    assert_eq!(
        repo.query_index(&Accessor::Runtime, &query)
            .await
            .expect("query")
            .len(),
        1
    );
}

/// A deliberately failing final operation must roll back both the preceding
/// object and its index projection. The reversed request identities also make
/// every SQL backend exercise its canonical lock/execution ordering while the
/// returned result order remains a separate contract.
async fn scenario_atomic_batch_rolls_back_objects_and_indexes(repo: &dyn StorageRepository) {
    let index = score_index();
    repo.install_index(&index).await.expect("install index");
    let first = id(Owner::System, "profiles", "z-first");
    let second = id(Owner::System, "profiles", "a-existing");
    repo.write(
        &Accessor::Runtime,
        WriteRequest::upsert(second.clone(), value(1), Permissions::public_read()),
    )
    .await
    .expect("seed conflict target");

    let error = repo
        .atomic_batch(vec![
            AtomicBatchOperation::Write {
                accessor: Accessor::Runtime,
                request: WriteRequest::upsert(first.clone(), value(7), Permissions::public_read())
                    .expecting(Precondition::MustNotExist),
                membership: None,
            },
            AtomicBatchOperation::Write {
                accessor: Accessor::Runtime,
                request: WriteRequest::upsert(second.clone(), value(2), Permissions::public_read())
                    .expecting(Precondition::MustNotExist),
                membership: None,
            },
        ])
        .await
        .expect_err("failing final mutation aborts the complete batch");
    assert_eq!(error.category(), ErrorCategory::Conflict);
    assert!(
        repo.read(&Accessor::Runtime, &first)
            .await
            .expect("read")
            .is_none(),
        "the preceding object is rolled back"
    );
    let filters = json!({"score": 7});
    let query = StorageIndexQuery::from_json_filters(
        index,
        filters.as_object().expect("object filters"),
        10,
    )
    .expect("query");
    assert!(
        repo.query_index(&Accessor::Runtime, &query)
            .await
            .expect("query")
            .is_empty(),
        "the preceding index membership is rolled back too"
    );
}

type ScenarioFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>;
type Scenario = (
    &'static str,
    fn(&dyn StorageRepository) -> ScenarioFuture<'_>,
);

/// Every scenario, as `(name, runner)` pairs, so both backends run the exact
/// same set. The explicit return-type annotation forces the unsizing coercion
/// to a trait-object future, and the `as fn(..)` cast turns each non-capturing
/// closure into a plain function pointer.
macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        vec![$((
            stringify!($name),
            (|repo| -> ScenarioFuture<'_> { Box::pin($name(repo)) })
                as fn(&dyn StorageRepository) -> ScenarioFuture<'_>,
        )),*]
    };
}

fn all_scenarios(supports_atomic_batch: bool) -> Vec<Scenario> {
    let mut scenarios = scenarios![
        scenario_owner_can_write_and_read_own_object,
        scenario_client_cannot_create_object_for_another_owner,
        scenario_client_cannot_create_system_object,
        scenario_non_owner_cannot_overwrite_owner_private_object,
        scenario_owner_only_object_is_invisible_to_other_clients_and_public,
        scenario_public_read_object_is_visible_to_everyone,
        scenario_runtime_only_object_is_hidden_from_clients_but_visible_to_runtime,
        scenario_create_only_precondition_rejects_duplicate,
        scenario_optimistic_version_match_round_trip,
        scenario_delete_with_matching_version_then_missing_conflicts,
        scenario_idempotent_delete_of_absent_object,
        scenario_list_rejects_zero_limit,
        scenario_list_paginates_with_cursor_and_filters_by_permission,
        scenario_list_collections_counts_every_object,
        scenario_index_query_filters_declared_json_fields_and_permissions,
        scenario_index_membership_exclusion_is_durable_and_atomic,
    ];
    // MongoDB deliberately has no portable multi-key retry contract yet. Keep
    // its generic storage suite on the shared single-object scenarios, with a
    // dedicated test below pinning the documented unsupported-batch boundary.
    if supports_atomic_batch {
        scenarios.push((
            stringify!(scenario_atomic_batch_rolls_back_objects_and_indexes),
            (|repo| -> ScenarioFuture<'_> {
                Box::pin(scenario_atomic_batch_rolls_back_objects_and_indexes(repo))
            }) as fn(&dyn StorageRepository) -> ScenarioFuture<'_>,
        ));
    }
    scenarios
}

// --- In-memory runs (always) ------------------------------------------------

#[tokio::test]
async fn in_memory_backend_satisfies_the_contract() {
    for (name, run) in all_scenarios(true) {
        // A fresh repository per scenario mirrors each scenario's clean-slate
        // assumptions.
        let repo = InMemoryStorageRepository::new();
        run(&repo).await;
        // `name` is only used for diagnostics if a scenario panics.
        let _ = name;
    }
}

#[test]
fn permission_codes_match_nakama_numbering() {
    assert_eq!(ReadPermission::NoRead.code(), 0);
    assert_eq!(ReadPermission::OwnerRead.code(), 1);
    assert_eq!(ReadPermission::PublicRead.code(), 2);
    assert_eq!(WritePermission::NoWrite.code(), 0);
    assert_eq!(WritePermission::OwnerWrite.code(), 1);
}

// --- SQLite run (always; embedded, no server) -------------------------------

mod sqlite {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::SqliteDatabase;

    #[tokio::test]
    async fn sqlite_backend_satisfies_the_contract() {
        // SQLite is embedded, so — unlike Postgres — this run needs no server and
        // is UN-gated: it exercises a real SQL backend on every `check.sh`. An
        // in-memory database keeps it hermetic (the provider forces a single
        // connection so all statements see the same database).
        let config = DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        };
        let db = SqliteDatabase::connect(&config)
            .await
            .expect("connect + migrate against an in-memory SQLite database");
        let repo = db.storage_repository();

        for (name, run) in all_scenarios(true) {
            // Isolate scenarios: they assume a clean slate and reuse fixed ids.
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("sqlite scenario: {name}");
            run(repo.as_ref()).await;
        }
    }

    #[tokio::test]
    async fn sqlite_caller_owned_batch_failure_rolls_back_to_savepoint() {
        let db = SqliteDatabase::connect(&DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect");
        let outer = id(Owner::System, "savepoint", "outer");
        let inner = id(Owner::System, "savepoint", "inner");
        let conflict = id(Owner::System, "savepoint", "conflict");
        db.storage_repository()
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(conflict.clone(), value(1), Permissions::public_read()),
            )
            .await
            .expect("seed conflict");
        let uow = db.begin().await.expect("begin");
        let repo = uow.storage_repository();
        repo.write(
            &Accessor::Runtime,
            WriteRequest::upsert(outer.clone(), value(1), Permissions::public_read()),
        )
        .await
        .expect("outer write");
        let error = repo
            .atomic_batch(vec![
                AtomicBatchOperation::Write {
                    accessor: Accessor::Runtime,
                    request: WriteRequest::upsert(
                        inner.clone(),
                        value(2),
                        Permissions::public_read(),
                    ),
                    membership: None,
                },
                AtomicBatchOperation::Write {
                    accessor: Accessor::Runtime,
                    request: WriteRequest::upsert(conflict, value(3), Permissions::public_read())
                        .expecting(Precondition::MustNotExist),
                    membership: None,
                },
            ])
            .await
            .expect_err("second batch write conflicts");
        assert_eq!(error.category(), ErrorCategory::Conflict);
        uow.commit().await.expect("commit unaffected outer work");
        let pooled = db.storage_repository();
        assert!(
            pooled
                .read(&Accessor::Runtime, &outer)
                .await
                .expect("read outer")
                .is_some()
        );
        assert!(
            pooled
                .read(&Accessor::Runtime, &inner)
                .await
                .expect("read inner")
                .is_none()
        );
    }

    #[tokio::test]
    async fn sqlite_overlapping_create_batches_have_one_winner() {
        let db = SqliteDatabase::connect(&DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect");
        let repo = db.storage_repository();
        let object = id(Owner::System, "concurrent", "same-key");
        let batch = |score| {
            vec![AtomicBatchOperation::Write {
                accessor: Accessor::Runtime,
                request: WriteRequest::upsert(
                    object.clone(),
                    value(score),
                    Permissions::public_read(),
                )
                .expecting(Precondition::MustNotExist),
                membership: None,
            }]
        };
        let (left, right) = tokio::join!(repo.atomic_batch(batch(1)), repo.atomic_batch(batch(2)));
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert!(
            repo.read(&Accessor::Runtime, &object)
                .await
                .expect("read winner")
                .is_some()
        );
    }
}

// --- Postgres run (opt-in via DATABASE_URL) ---------------------------------

mod postgres {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::PgDatabase;

    fn test_database_url() -> Option<String> {
        std::env::var("DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
            .filter(|url| !url.trim().is_empty())
    }

    #[tokio::test]
    async fn postgres_backend_satisfies_the_contract() {
        let Some(url) = test_database_url() else {
            eprintln!(
                "skipping Postgres storage contract: set DATABASE_URL or \
                 CITADEL_TEST_DATABASE_URL to run it"
            );
            return;
        };

        let config = DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        };
        let db = PgDatabase::connect(&config)
            .await
            .expect("connect + migrate against the test Postgres");
        let repo = db.storage_repository();

        for (name, run) in all_scenarios(true) {
            // Isolate scenarios: they assume a clean slate and reuse fixed ids.
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("postgres scenario: {name}");
            run(repo.as_ref()).await;
        }
    }

    #[tokio::test]
    async fn postgres_caller_owned_batch_failure_rolls_back_to_savepoint() {
        let Some(url) = test_database_url() else {
            eprintln!("skipping Postgres savepoint batch test: no test database URL");
            return;
        };
        let db = PgDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect");
        db.reset_storage_for_tests().await.expect("reset");
        let outer = id(Owner::System, "savepoint", "outer");
        let inner = id(Owner::System, "savepoint", "inner");
        let conflict = id(Owner::System, "savepoint", "conflict");
        db.storage_repository()
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(conflict.clone(), value(1), Permissions::public_read()),
            )
            .await
            .expect("seed conflict");
        let uow = db.begin().await.expect("begin");
        let repo = uow.storage_repository();
        repo.write(
            &Accessor::Runtime,
            WriteRequest::upsert(outer.clone(), value(1), Permissions::public_read()),
        )
        .await
        .expect("outer write");
        let error = repo
            .atomic_batch(vec![
                AtomicBatchOperation::Write {
                    accessor: Accessor::Runtime,
                    request: WriteRequest::upsert(
                        inner.clone(),
                        value(2),
                        Permissions::public_read(),
                    ),
                    membership: None,
                },
                AtomicBatchOperation::Write {
                    accessor: Accessor::Runtime,
                    request: WriteRequest::upsert(conflict, value(3), Permissions::public_read())
                        .expecting(Precondition::MustNotExist),
                    membership: None,
                },
            ])
            .await
            .expect_err("second batch write conflicts");
        assert_eq!(error.category(), ErrorCategory::Conflict);
        uow.commit().await.expect("commit unaffected outer work");
        let pooled = db.storage_repository();
        assert!(
            pooled
                .read(&Accessor::Runtime, &outer)
                .await
                .expect("read outer")
                .is_some()
        );
        assert!(
            pooled
                .read(&Accessor::Runtime, &inner)
                .await
                .expect("read inner")
                .is_none()
        );
    }

    #[tokio::test]
    async fn postgres_overlapping_create_batches_have_one_winner() {
        let Some(url) = test_database_url() else {
            eprintln!("skipping Postgres concurrent batch test: no test database URL");
            return;
        };
        let db = PgDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect");
        db.reset_storage_for_tests().await.expect("reset");
        let repo = db.storage_repository();
        let object = id(Owner::System, "concurrent", "same-key");
        let batch = |score| {
            vec![AtomicBatchOperation::Write {
                accessor: Accessor::Runtime,
                request: WriteRequest::upsert(
                    object.clone(),
                    value(score),
                    Permissions::public_read(),
                )
                .expecting(Precondition::MustNotExist),
                membership: None,
            }]
        };
        let (left, right) = tokio::join!(repo.atomic_batch(batch(1)), repo.atomic_batch(batch(2)));
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert!(
            repo.read(&Accessor::Runtime, &object)
                .await
                .expect("read winner")
                .is_some()
        );
    }
}

// --- MongoDB run (opt-in via the authenticated replica-set harness) --------

mod mongodb {
    use super::*;
    use ::mongodb::bson::{Document, doc};
    use citadel::config::DatabaseConfig;
    use citadel::repository::{Backend, MongoDatabase};

    async fn connect() -> Option<MongoDatabase> {
        let url = std::env::var("CITADEL_TEST_MONGODB_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        MongoDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .ok()
    }

    async fn fail_next(db: &MongoDatabase, command: &str) {
        db.admin_database_for_tests()
            .run_command(doc! {
                "configureFailPoint": "failCommand",
                "mode": { "times": 1 },
                "data": {
                    "failCommands": [command],
                    "errorCode": 2_i32,
                    "failInternalCommands": true,
                },
            })
            .await
            .expect("enable one-shot storage failure");
    }

    #[tokio::test]
    async fn mongodb_backend_satisfies_the_contract() {
        let Some(db) = connect().await else {
            eprintln!("skipping MongoDB storage contract: set CITADEL_TEST_MONGODB_URL");
            return;
        };
        let repo = db.storage_repository();
        for (name, run) in all_scenarios(false) {
            db.clear_storage_data_for_tests()
                .await
                .expect("clear Mongo storage projections");
            eprintln!("mongodb scenario: {name}");
            run(repo.as_ref()).await;
        }
    }

    #[tokio::test]
    async fn mongodb_atomic_batch_returns_the_documented_unsupported_error() {
        let Some(db) = connect().await else {
            eprintln!(
                "skipping MongoDB atomic-batch boundary contract: CITADEL_TEST_MONGODB_URL is unset"
            );
            return;
        };
        let repo = db.storage_repository();
        let error = repo
            .atomic_batch(vec![AtomicBatchOperation::Write {
                accessor: Accessor::Runtime,
                request: WriteRequest::upsert(
                    id(Owner::System, "atomic-batch", "unsupported"),
                    value(1),
                    Permissions::public_read(),
                ),
                membership: None,
            }])
            .await
            .expect_err("MongoDB atomic batches remain explicitly unsupported");
        assert_eq!(error.category(), ErrorCategory::Validation);
        assert_eq!(
            error.message(),
            "atomic storage batches are not supported by the MongoDB backend"
        );
    }

    #[tokio::test]
    async fn mongodb_storage_transactions_rollback_intermediate_projection_failures() {
        let Some(db) = connect().await else {
            eprintln!(
                "skipping MongoDB storage rollback contract: CITADEL_TEST_MONGODB_URL is unset"
            );
            return;
        };
        db.clear_storage_data_for_tests()
            .await
            .expect("clear isolated storage projections");
        let repo = db.storage_repository();
        let alice = user("alice");
        let object_id = id(Owner::user(alice.clone()), "profiles", "atomic");
        let index = score_index();
        repo.install_index(&index).await.expect("install index");
        repo.write(
            &Accessor::User(alice.clone()),
            WriteRequest::upsert(object_id.clone(), value(1), Permissions::public_read()),
        )
        .await
        .expect("seed object + membership");

        // The object replacement succeeds before the injected membership insert
        // fails. The transaction must abort both projection changes.
        fail_next(&db, "insert").await;
        let error = repo
            .write(
                &Accessor::User(alice.clone()),
                WriteRequest::upsert(object_id.clone(), value(2), Permissions::public_read()),
            )
            .await
            .expect_err("injected membership insert fails the write");
        assert_eq!(error.category(), ErrorCategory::Database);
        assert_eq!(
            repo.read(&Accessor::Runtime, &object_id)
                .await
                .expect("read after aborted write")
                .expect("seed object remains")
                .value
                .as_json(),
            &json!({ "score": 1 }),
            "object replacement rolled back with its memberships"
        );
        assert_eq!(
            db.database_for_tests()
                .collection::<Document>("storage_index_memberships")
                .count_documents(doc! { "object_key": "atomic" })
                .await
                .expect("count memberships after aborted write"),
            1,
            "prior index membership remains intact"
        );

        // Membership removal succeeds before the injected CAS object removal
        // fails. A failed delete therefore proves rollback in the other order.
        fail_next(&db, "findAndModify").await;
        let error = repo
            .delete(
                &Accessor::User(alice.clone()),
                &object_id,
                Precondition::Any,
            )
            .await
            .expect_err("injected object CAS deletion fails");
        assert_eq!(error.category(), ErrorCategory::Database);
        assert!(
            repo.read(&Accessor::Runtime, &object_id)
                .await
                .expect("read after aborted delete")
                .is_some(),
            "object survives an aborted delete"
        );
        assert_eq!(
            db.database_for_tests()
                .collection::<Document>("storage_index_memberships")
                .count_documents(doc! { "object_key": "atomic" })
                .await
                .expect("count memberships after aborted delete"),
            1,
            "membership removal rolled back with the object"
        );
    }
}
