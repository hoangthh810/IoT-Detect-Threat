# RULES.md

These rules apply to future AI coding sessions in this repository.

1. Always prioritize the MVP.
2. Do not implement an extension unless the current task explicitly requests it.
3. Do not add a framework, service, dependency, abstraction, or infrastructure component unless it directly solves the current task.
4. Prefer simple, readable, and easy-to-debug Python/PySpark.
5. Avoid premature abstraction.
6. Do not create a factory, manager, service layer, repository pattern, interface, custom framework, wrapper, or helper class when a simple function or module is sufficient.
7. Create an abstraction only for a demonstrated repeated need, not for a hypothetical future use case.
8. Offline training and streaming inference must reuse the same preprocessing logic to prevent training-serving skew.
9. Never train a model inside streaming inference.
10. The producer is a CICIoT2023 dataset replay simulator, not a packet generator.
11. The MVP uses binary `Normal`/`Attack` classification; multiclass classification is an extension.
12. Never present `ground_truth`, `scenario`, or a simulator attack label as the model's prediction.
13. During development, use a small sample or partition first and increase data volume only after the pipeline works.
14. Evaluate Attack detection with Recall, F1, and a confusion matrix; do not rely on Accuracy alone.
15. Do not set an arbitrary Accuracy or F1 target before obtaining experimental results.
16. Every task must have a clear Definition of Done.
17. Do not modify files unrelated to the current task.
18. Before adding a dependency, verify that the standard library or an existing dependency is not sufficient.
19. Do not optimize for an imagined future scale.
20. Keep configuration simple; prefer environment variables or one small configuration file when configuration is needed.
21. A basic dashboard is part of the MVP; advanced dashboard behavior is an extension.
22. Use only one dashboard framework. The planned default is Streamlit; do not add Plotly Dash as a second framework.
23. Do not build a separate REST API, WebSocket server, or microservice for the dashboard when a simpler data path satisfies the MVP.
24. Console output and logs are acceptable while debugging, but the final demo must include the visual dashboard defined by the MVP.
25. Do not add Kubernetes, cloud infrastructure, microservices, Airflow, Redis, complex CI/CD, or an enterprise observability platform unless explicitly requested.
26. When a requirement is unclear, choose the simplest implementation that still satisfies the MVP and record the assumption briefly.
27. A stable end-to-end pipeline is more important than using many technologies or architecture layers.
