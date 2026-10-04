# Contributing to Retail Order System

Thanks for your interest in improving this project! 🎉

## Development setup

1. Install the prerequisites:
   - JDK 17+
   - Maven 3.6+
   - Docker Desktop (required for the Testcontainers integration tests)
2. Clone the repository and build:

   ```bash
   git clone https://github.com/hulamanisrinish-cpu/Retail-Order-System.git
   cd Retail-Order-System
   mvn clean install
   ```

3. Start the supporting infrastructure (PostgreSQL, Kafka, Kafka UI):

   ```bash
   docker-compose up -d
   ```

## Running the tests

```bash
# Full suite (run `docker-compose up -d` first for the local-env suite)
mvn test

# Only the self-contained Testcontainers suites (Docker required)
mvn test -Dtest='OrderControllerIntegrationTestWith*'
```

## Contribution workflow

1. Fork the repo and create a feature branch:
   `git checkout -b feature/my-feature`
2. Make your changes and ensure the tests pass.
3. Keep commits small and descriptive.
4. Open a pull request against `main` — CI must be green before review.

## Code style

- Follow the existing code layout; keep classes small and focused.
- Prefer constructor injection over field injection.
- Public REST changes must be reflected in the OpenAPI annotations.

## Reporting issues

Open a GitHub issue with steps to reproduce, expected vs. actual behavior,
and relevant logs.

---

© 2026 Srinish Hulamani — released under the [MIT License](LICENSE).
