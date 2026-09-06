# EventForge console

An operator view over captured failures: what failed, replaying it, and a record of who replayed
what. React + Vite + TypeScript (strict).

## Running it

The console is a client for order-service, so the stack has to be up:

```bash
make up          # from the repository root: Docker infra + the four services
cd console
npm install
npm run dev      # http://localhost:5173
```

Sign in with the seeded development credentials — `operator`/`operator` or `viewer`/`viewer`. They
are unencoded `{noop}` values in each service's `application.yml`; the authorization *rule* is the
thing this project proves, not the credential handling.

Requests go to `/api/*` and are proxied to `localhost:8081` by the Vite dev server. That is
deliberate: the alternative is relaxing CORS on the services to accommodate a development tool,
which is how permissive CORS ends up in production.

## API types are generated, not written

`src/api/schema.ts` is generated from `openapi/order-service.json`:

```bash
npm run generate:types
```

The spec itself is committed, and regenerated from the running service by a test:

```bash
./gradlew :order-service:test --tests '*OpenApiSpecSnapshotTest*' -DupdateOpenApiSpec=true
```

Committing the spec keeps type generation a pure file transform — no Postgres, no Kafka, no
service — because a generation step that needs the whole stack is one people skip, and skipped
regeneration is how a generated client silently becomes a stale hand-written one. That trade is
only safe because `OpenApiSpecSnapshotTest` fails when the committed spec stops matching what the
service serves. Change a controller without regenerating and CI tells you.

Do not hand-edit `src/api/schema.ts` or `openapi/order-service.json`. A diff in either during
review is the API contract changing, which is worth reading.

### One thing the generated types get wrong

Every field is optional (`status?: string`), because the Java records do not declare which fields
are required in the OpenAPI description. The console handles this by rendering a placeholder for
missing values rather than asserting non-null — a missing value shows as missing instead of as an
empty cell that looks like data. Annotating the Java side with required-ness would remove the
problem at its source; that has not been done.

## Tests

```bash
npm run typecheck
npm run lint
npm run e2e        # needs `make up` first
```

The Playwright suite runs against the **real** order-service, not a mocked API. The claim under
test is that the *server* denies an unauthorized replay; stubbing the API would assert that the
stub returns `403` and prove nothing about the server. Each test anchors its assertions to
something the server owns — the status it returned, the row it did or did not modify, the audit row
it did or did not write — so none of them can pass vacuously.

Fixtures insert directly into order-service's database (`e2e/fixtures.ts`), because there is
deliberately no endpoint that creates a captured failure: those rows are written only by the code
path that captures a genuine consumer failure.

**CI does not run these.** It typechecks, lints and builds the console; the browser tests need
Docker infra plus four JVM processes, which is a heavier and more fragile CI job than it would buy.
Run them locally before changing authorization behaviour.
