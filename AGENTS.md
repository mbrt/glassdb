# Coding Agents Instructions

## Development

### Tests

Always use `make test` to run tests. This runs linting, type checking, and format checks:

```bash
make test
```

- Test interfaces and intended behavior instead of internals
- Prefer integration tests to mocks as much as possible

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
