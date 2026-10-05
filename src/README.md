# Приклад реалізації: Avalonia 12 + gRPC

Робоча реалізація дизайну з [../design/sync-design.md](../design/sync-design.md) на .NET 10, SQLite і EF Core 10, Avalonia 12.1.

| Проєкт | Що це |
|---|---|
| `Replication.Core` | Ядро реплікації, яке можна скопіювати у свій проєкт. Без залежності від Avalonia. Власник (тригери, `SyncService`, Apply, знімок, очищення tombstones) і клієнт (одна БД з `InstanceId`, `_sync_outbox`, `WriteRouter`, `SyncAgent`), gRPC-контракт `Protos/sync.proto`, лічильник байтів на дроті. |
| `Sample.Domain` | Модель прикладу: `BaseEntity` (Id, CreatedOn, ModifiedOn) і `Category`, `Item`, `LogEntry`. Сутності нічого не знають про синк. |
| `Sample.Lab` | «Лабораторія»: власник і два клієнти в одному процесі, кожен зі своїм SQLite-файлом, справжній gRPC через localhost. Тут же 17 сценаріїв з демо-сторінки. |
| `Sample.App` | Avalonia-додаток: лабораторія, окремо власник (з вікном або headless) і окремо клієнт. |
| `Replication.Tests` | Усі сценарії демо-сторінки як тести на справжньому ядрі, плюс тести курсорів і порожньої репліки. |

## Запуск

```bash
dotnet run --project Sample.App
```

Відкривається лабораторія: зверху вибір сценарію і кнопка «Виконати крок N», під нею власник і два клієнти, внизу журнал подій. Кнопка «перевірити» чекає, поки все доїде, і порівнює кожну репліку з власником.

Окремі процеси, наприклад власник на одній машині і клієнти на інших:

```bash
dotnet run --project Sample.App -- --owner --headless --port 5005 --db owner.db --any
dotnet run --project Sample.App -- --client --connect http://10.0.0.5:5005 --db client1.db --name "Клієнт 1"
```

`--owner` без `--headless` показує вікно власника. `--any` слухає всі інтерфейси, без нього тільки 127.0.0.1.

Тести:

```bash
dotnet test Replication.Tests
```

## Лічильник байтів

У кожного клієнта два числа: ↑ клієнт → власник і ↓ власник → клієнт. Це байти на TCP-з'єднанні клієнта: HTTP/2-кадри з gzip-стисненими повідомленнями gRPC, заголовками і пінгами (`Net/ShapedStream.cs`, `Net/WireMeter.cs`). Під ними розгортається розбивка за типами повідомлень gRPC у нестисненому вигляді, щоб видно було, що дає стиснення. У панелі власника сума по клієнтах.

Той самий шар емулює мережу: у клієнта є профіль «Цільова: 1,5 с в кожен бік, ↑ 70 кбіт/с, ↓ 2 Мбіт/с» з розділу 16 дизайну.

## Як підключити ядро у свій проєкт

1. Скопіювати `Replication.Core` (або послатися на нього).
2. У `OnModelCreating` того самого DbContext, що на власнику і на клієнті, останнім рядком:

   ```csharp
   modelBuilder.UseReplication(t => typeof(BaseEntity).IsAssignableFrom(t));
   ```

   Службові колонки (`SyncVersion`, `SyncBase`, `SyncMask`, `SyncOrigin`, `InstanceId`) додаються shadow-властивостями, `BaseEntity` лишається як є. PK має бути `Guid Id` (розділ 5.1).
3. Власник, після міграцій:

   ```csharp
   var store = new OwnerStore("owner.db", SyncModel.From(db));
   store.Install();                                   // службові таблиці, тригери, файл версії
   var host = await OwnerHost.StartAsync(store, 5005, listenAnywhere: true);
   ```

   Автоматика пише як завжди: SaveChanges, ExecuteUpdate, ExecuteDelete, сирий SQL. Усе ловлять тригери.
4. Клієнт, після міграцій:

   ```csharp
   var replication = new ClientReplication("client.db", SyncModel.From(db));
   replication.Install();
   options.AddInterceptors(replication.Router);       // у DbContextOptions клієнта
   var agent = replication.Connect("http://10.0.0.5:5005", "Склад");
   agent.LinkEnabled = true;
   ```

   Нові записи отримують інстанс через `db.Items.Add(item).SetInstance(agent.InstanceId!)`. Читання це звичайні запити з фільтром `EF.Property<string>(x, "InstanceId")`. Масове видалення за умовою: `agent.DeleteWhereAsync("Item", new Predicate("Status", "архів"))`, перенесення в архів: `agent.ArchiveAsync(...)`.

## Де дизайн реалізовано інакше або доповнено

- **`_sync_outbox.pk`** може бути NULL: у дій за предикатом і перенесення в архів нема одного ключа.
- **Розмір пачки досинхронізації** задається в рядках (`OwnerOptions.CatchupBatchRows`, 500 за замовчуванням), а не в байтах; лабораторія ставить 3 для сценаріїв з відрізками, як на демо-сторінці.

Батько архівного рядка (11.6) і порожня таблиця в handshake (6.2) реалізовані за дизайном і покриті тестами `ArchiveParentTests` і `EmptyReplicaTests`.