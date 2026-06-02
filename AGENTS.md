# Coding Agents Instructions

## Development

Create an ADR (in `docs/adr/xxx-adr-slug.md`) for every important feature
implemented.

### Tests

Always use `make test` to run tests. This runs linting, type checking, and format checks:

```bash
make test
```

- Test interfaces and intended behavior instead of internals
- Prefer integration tests to mocks as much as possible
- Add deterministic tests for bugs discovered by fuzz tests

### Formatting Code

Use `make format` to auto-format and fix linting issues:

```bash
make format
```

### Before Committing

Run `make test` to ensure all checks pass before committing changes.

### Dependencies

Always tidy after adding or removing dependencies:

```bash
go get $package
go mod tidy
```
