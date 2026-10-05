# Реплікація для desktop-застосунку

Дизайн клієнт-серверного режиму синхронізації для desktop-застосунку на Avalonia (.NET 10) і SQLite.

- [design/sync-design.md](design/sync-design.md): дизайн-документ синхронізації.
- [design/sim-findings-review.md](design/sim-findings-review.md): огляд слабких місць, знайдених симуляцією, і рішення щодо них.
- [demo/sync-demo.html](demo/sync-demo.html): інтерактивна демо механізму синхронізації (відкрити у браузері).
- [demo/sim/](demo/sim/): код симуляції слабких місць (`node run.js`), результати (`results.json`, `results.txt`) і звіт `weak-spots.html`.
- [src/](src/README.md): приклад реалізації на .NET 10, Avalonia 12 і gRPC: ядро реплікації, яке можна скопіювати у свій проєкт, лабораторія з власником і двома клієнтами, сценарії демо-сторінки як тести.
