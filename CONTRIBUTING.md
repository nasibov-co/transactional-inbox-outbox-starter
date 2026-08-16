# Contributing

Thank you for your interest in contributing to Transactional Inbox/Outbox Starter!

Contributions of all kinds are welcome, including bug reports, documentation improvements, new features, tests, and fixes.

## Getting Started

### Requirements

* JDK 25
* Git

Clone the repository:

```bash
git clone https://github.com/nasibov-co/transactional-inbox-outbox-starter.git
cd transactional-inbox-outbox-starter
```

Build the project:

```bash
./gradlew build
```

On Windows:

```powershell
gradlew.bat build
```

## Running Tests

Run all tests:

```bash
./gradlew test
```

Run tests for a specific module:

```bash
./gradlew :<module-name>:test
```

Please make sure all tests pass before submitting a pull request.

## Reporting Bugs

Before opening a new issue, check whether the problem has already been reported.

When reporting a bug, include:

* Starter version
* Spring Boot version
* Java version
* Database and driver version
* Persistence technology: JDBC, JPA, or R2DBC
* Relevant configuration
* Steps to reproduce the problem
* Expected behavior
* Actual behavior
* Relevant logs or stack traces

Whenever possible, provide a minimal reproducible example.

## Proposing Changes

For significant changes or new features, please open an issue first. Describe the problem, the proposed solution, and any alternatives you considered.

Small fixes and documentation improvements can be submitted directly as pull requests.

## Pull Requests

Before submitting a pull request:

* Keep the changes focused on a single problem.
* Follow the existing code style and project structure.
* Add or update tests for changed behavior.
* Update documentation when configuration or public APIs change.
* Avoid unrelated refactoring.
* Make sure the project builds successfully.
* Provide a clear description of what was changed and why.

## Commit Messages

Use concise and descriptive commit messages.

Examples:

```text
Add JDBC inbox message cleanup
Fix R2DBC polling transaction handling
Update outbox configuration documentation
```

## Backward Compatibility

This project is used as a dependency by other applications. Avoid breaking changes to public APIs and configuration properties whenever possible.

If a breaking change is necessary, clearly describe it in the issue and pull request.

## Security Issues

Please do not publicly disclose security vulnerabilities through GitHub Issues.

Instead, use GitHub's private vulnerability reporting feature if it is enabled for the repository.

## License

By contributing to this project, you agree that your contributions will be licensed under the Apache License 2.0.
