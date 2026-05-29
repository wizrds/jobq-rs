# AGENTS Guidelines

This repository follows these guidelines for contributions by AI agents or humans:

1. **Commit Messages**: Use [Conventional Commits](https://www.conventionalcommits.org/) format. Examples include:
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation changes
   - `test:` for test-related changes
   - `chore:` for maintenance tasks

2. **Simplicity First**: Prefer simpler implementations over overly complex solutions.

3. **Run Tests**: Always run tests before committing to ensure functionality and catch regressions. Use `cargo test` to run tests.

4. **Uniform Structure**: Maintain a consistent code structure across modules so files and packages are easy to navigate. Ensure code style is uniform.

5. **Explain Why**: Add comments explaining *why* something is done if it is not obvious from code alone.

6. **Documentation**: Update relevant documentation when making changes that affect usage or behavior. Ensure doc comments and docstrings are clear and follow Rust documentation conventions.

7. **Branch Names**: Use '_type_/_short_topic_' convention for new branches (e.g. feat/add-s3-backup).

9. **Style & Formatting**: Use `clippy` for linting and `rustfmt` for formatting to ensure code quality and consistency.

10. **Security**: Run security checks using `cargo audit` to identify vulnerabilities in dependencies.