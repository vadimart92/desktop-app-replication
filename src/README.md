# Приклад реалізації: Avalonia 12 + gRPC

Робоча реалізація дизайну з [../design/sync-design.md](../design/sync-design.md) на .NET 10, SQLite і EF Core 10, Avalonia 12.1.

| Проєкт | Що це |
|---|---|
| `Replication` | Ядро реплікації, яке можна скопіювати у свій проєкт. Без залежності від Avalonia. Власник (тригери, gRPC-сервіс `SyncGrpcService`, Apply, знімок, очищення tombstones) і клієнт (одна БД з `InstanceId`, `_sync_outbox`, `WriteRouter`, `SyncAgent`), gRPC-контракт `Protos/sync.proto`. |
| `Sample.Domain` | Модель прикладу: `BaseEntity` (Id, CreatedOn, ModifiedOn) і `Category`, `Item`, `LogEntry`, `StockObject` (з комплексним типом `ObjectSettings`). Сутності нічого не знають про синк. |
| `Sample.Lab` | «Лабораторія»: власник і два клієнти в одному процесі, кожен зі своїм SQLite-файлом, справжній gRPC через localhost. Тут же 18 сценаріїв з демо-сторінки (вільний режим без кроків і 17 зі кроками). `TwoOwnerLab`: два власники з різними початковими даними і один клієнт, під'єднаний до обох. У `Net/` лічильник байтів на дроті й емуляція мережі для клієнтів лабораторії. |
| `Sample.App` | Avalonia-додаток: лабораторія, окремо власник (з вікном або headless) і окремо клієнт. |
| `Replication.Tests` | Усі сценарії демо-сторінки як тести на справжньому ядрі, плюс тести ядра за розділами дизайну (курсори і `_sync_floor`, Apply і відповіді на нього, WriteRouter, архів і FK, очищення tombstones, знімок, увімкнення синку на наявній БД, сесія Subscribe, порожня репліка) і view model панелі власника (посилається на `Sample.App`). NUnit. У `Integration/` інтеграційні тести з явними перевірками стану обох БД: підключення ядра до DbContext, версії і маски на власнику, знімок і потік до клієнта, запис з клієнта, конфлікти, перезапуски і відновлення, а також EF-міграція схеми (перебудова таблиці, нова колонка, нова таблиця) поверх працюючої реплікації, і один клієнт на двох власниках (`TwoOwnerTests`). |

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

## Стиль коду і перевірки

Код написано за конвенціями Microsoft: [coding style dotnet/runtime](https://github.com/dotnet/runtime/blob/main/docs/coding-guidelines/coding-style.md) і [C# coding conventions](https://learn.microsoft.com/dotnet/csharp/fundamentals/coding-style/coding-conventions). Правила записані в `.editorconfig` у корені. Відступ від них один: не-ASCII символи в рядках лишаються як є, без `\uXXXX`, бо тексти журналу й інтерфейсу українською. `EnforceCodeStyleInBuild` і `TreatWarningsAsErrors` увімкнені, тож більшість порушень ламає збірку.

Перед комітом усі чотири перевірки мають пройти:

```bash
dotnet build Replication.slnx
dotnet format Replication.slnx --verify-no-changes
dotnet test Replication.Tests
dotnet tool restore && dotnet slopwatch analyze -d .. --fail-on warning
```

`dotnet format --verify-no-changes` потрібен окремо: порядок `using`, `this.` і ключові слова замість імен типів BCL збірка не перевіряє. Slopwatch (локальний інструмент `Slopwatch.Cmd` з `dotnet-tools.json`) ловить вимкнені тести, придушені попередження і порожні `catch`; наявні випадки записані в `.slopwatch/baseline.json`.

## Лічильник байтів

У кожного клієнта два числа: ↑ клієнт → власник і ↓ власник → клієнт. Це байти на TCP-з'єднанні клієнта: HTTP/2-кадри з gzip-стисненими повідомленнями gRPC, заголовками і пінгами (`Sample.Lab/Net/ShapedStream.cs`, `Sample.Lab/Net/WireMeter.cs`). У панелі власника сума по клієнтах.

Той самий шар емулює мережу (`Sample.Lab/Net/NetworkProfile.cs`): у клієнта є профіль «Цільова: 1,5 с в кожен бік, ↑ 70 кбіт/с, ↓ 2 Мбіт/с» з розділу 16 дизайну.

Ядро про це не знає: `SyncAgent` приймає необов'язковий `wrapStream` (`Func<Stream, Stream>`), який обгортає TCP-потік кожного з'єднання, і `ClientNode.Connect` передає туди `ShapedStream`. Тести так само можуть підставити власну обгортку, щоб рахувати байти або втручатися в «мережу» (`StreamHookTests`).

## Як підключити ядро у свій проєкт

1. Скопіювати `Replication` (або послатися на нього).
2. У `OnModelCreating` того самого DbContext, що на власнику і на клієнті, останнім рядком:

   ```csharp
   modelBuilder.UseReplication(t => typeof(BaseEntity).IsAssignableFrom(t));
   ```

   Службові колонки (`SyncVersion`, `SyncBase`, `SyncMask`, `SyncOrigin`, `InstanceId`) додаються shadow-властивостями, `BaseEntity` лишається як є. PK має бути `Guid Id` (розділ 5.1). Комплексні типи EF (`[ComplexType]`) реплікуються: їхні властивості лежать у колонках таблиці власника (`Settings_Description`, `Settings_StockLevel`) і є звичайними колонками даних (біти маски, тригери, передача).
3. Власник, після міграцій:

   ```csharp
   services.AddOwnerServices("owner.db", SyncModel.From(db), new OwnerOptions());
   services.AddGrpc().ConfigureSyncGrpcServiceOptions();     // ліміти повідомлень і стиснення gRPC
   kestrel.ConfigureSyncKestrelServerOptions();              // у ConfigureKestrel, ліміти Kestrel
   app.MapSyncGrpcService();
   // ... міграції застосунку
   app.Services.InstallOwnerStore();                         // службові таблиці, тригери, файл версії
   await app.StartAsync();                            // адреса, порт і TLS за застосунком, див. Sample.Lab/OwnerHost.cs
   ```

   Автоматика пише як завжди: SaveChanges, ExecuteUpdate, ExecuteDelete, сирий SQL. Усе ловлять тригери.

   Журнал власника йде через `ILogger` з Microsoft.Extensions.Logging хоста: провайдери, фільтри і рівні налаштовує застосунок. Категорії:

   | Категорія | Що пише |
   |---|---|
   | `Replication.Owner.SubscribeSession` | Start, досинхронізація (рядок на кожну пачку), онлайн-пачки, NeedFull, `[client_id]` на початку рядка |
   | `Replication.Owner.SyncGrpcService` | Apply (рядок на кожен виклик), продовження завантаження знімка |
   | `Replication.Owner.SnapshotStore` | VACUUM INTO, невдале видалення старого знімка |
   | `Replication.Owner.SyncHousekeepingJobsService` | очищення tombstones, збої фонової задачі |

   Рівні: `Information` для звичайного ходу синку (його багато: кожна пачка і кожен Apply), `Warning` для SchemaMismatch, SnapshotRequired, відставання онлайну з поверненням до досинхронізації, Apply з Rejected/Ignored/changed і невдалого видалення знімка, `Error` для збоїв фонової задачі. У робочому застосунку `Information` зазвичай варто приглушити, наприклад в `appsettings.json`:

   ```json
   {
     "Logging": {
       "LogLevel": {
         "Default": "Information",
         "Replication.Owner": "Warning"
       }
     }
   }
   ```

   Для розбору проблем з окремим клієнтом можна лишити `Information` лише для `Replication.Owner.SubscribeSession`.
4. Клієнт, після міграцій:

   ```csharp
   services.AddClientServices("client.db", SyncModel.From(db));   // ClientStore і WriteRouter, один раз на клієнта
   options.AddInterceptors(validation, router);                   // у DbContextOptions клієнта, WriteRouter з DI останнім
   // ... міграції застосунку
   app.Services.InstallClientStore();                             // IServiceProvider: службові таблиці клієнта
   var agent = new SyncAgent(store, loggerFactory.CreateLogger<SyncAgent>(), "http://10.0.0.5:5005", "Склад", options); // по одному на власника
   agent.Start();
   agent.LinkEnabled = true;
   ```

   Агент пише в `ILogger<SyncAgent>` (категорія `Replication.Client.SyncAgent`; без логера не пише нікуди), кожен рядок починається з `[ім'я агента]`. Рівні: `Information` для звичайного ходу (Start, кожна пачка, кожен Apply і ApplyReply, знімок), `Warning` для обриву зв'язку, вимкненого зв'язку, SchemaMismatch, SnapshotRequired, відставання каналу, повтору Apply і перенесень в архів, що лишились на власнику, `Error` для другого агента на ту саму репліку, загубленого ApplyReply, відкинутої черги старої репліки і відхилених дій. `WriteRouter` пише кожен запис у чергу в `Replication.Client.WriteRouter` (`Information`, рядок починається з `[instance]`) через `ILoggerFactory`, зареєстрований у тих самих сервісах, що й `AddClientServices`. Приглушується так само, наприклад `"Replication.Client": "Warning"` поруч із `"Replication.Owner"` у `appsettings.json`.

   Роутер стоїть після власних перехоплювачів застосунку (тут `validation`). EF не повідомляє жодному перехоплювачу, що наступний за ним кинув виняток у `SavingChanges`, тож перехоплювач після роутера, який відхилив збереження, лишив би транзакцію роутера, а з нею блокування запису клієнтської БД, до наступного збереження на цьому контексті.

   Агент питає власника `Identity` на початку кожної сесії, тому `Start` не чекає власника, а адреса може змінюватись між запусками. `agent.InstanceId` стає відомим на першому зв'язку і до того null. Один інстанс веде один агент `ClientStore`: другий агент, що дійшов до того самого власника (навіть за іншою адресою), переходить у `AgentState.Duplicate` і не підписується. На `Duplicate` і `SchemaMismatch` агент вимикає `LinkEnabled` і лишає стан; застосунок вмикає зв'язок знову, коли причину усунуто. Нова репліка бере знімок; `AgentOptions.SkipSnapshotOnEmptyReplica` замість цього заповнює її потоком.

   Нові записи отримують інстанс через `db.Items.Add(item).SetInstance(agent.InstanceId!)`. Читання це звичайні запити з фільтром `EF.Property<string>(x, "InstanceId")`. Масове видалення за умовою: `agent.DeleteWhereAsync("Item", new Predicate("Status", "архів"))`, перенесення в архів: `agent.ArchiveAsync(...)`.

## Де дизайн реалізовано інакше або доповнено

- **`_sync_outbox.pk`** може бути NULL: у дій за предикатом і перенесення в архів нема одного ключа.
- **Розмір пачки досинхронізації** задається в рядках (`OwnerOptions.CatchupBatchRows`, 500 за замовчуванням), а не в байтах; лабораторія ставить 3 для сценаріїв з відрізками, як на демо-сторінці.
- **Черга при новому знімку.** Знімок і `SkipSnapshotOnEmptyReplica` відкидають чергу інстансу (з повідомленням), а не переносять її (10.3).
- **Зміна версії схеми** лише зупиняє агента в `SchemaMismatch`; знімок за нею не береться, курсори продовжуються.
- **Перенесення в архів** йде набором ключів з замиканням по FK, а не предикатом; відповідь `Applied` несе `changed[]` і `children[]` замість `Rejected: changed`. За предикатом передається лише `DeleteWhereAsync`.
- **`Progress`** власник шле з `done = true` наприкінці досинхронізації, а залишок змін це одне число `Batch.remaining`. Онлайн-потік, що відстав від голови більше ніж на `OwnerOptions.OnlineLagVersions` (1200), без обриву повертається до досинхронізації: власник шле `Progress` з `done = false` і `remaining`, клієнт переходить у `AgentState.Catchup`.
- **Розмір онлайн-пачки** `OwnerOptions.OnlineBatchRows` 500 рядків, як і пачки досинхронізації (у дизайні було 2000).
- **Додатково до дизайну:** стани `Connecting` і `Duplicate` (один агент на інстанс у `ClientStore`), таблиця `_sync_columns` (перенумерація масок), колонка `_sync_meta.clock_offset`; `<БД>.syncstate` це файл версії з 5.3.
- **Не реалізовано** (позначено в дизайні): маска `BLOB` для таблиць понад 63 колонки (комплексні типи розгортаються в колонки і теж рахуються), ліміт відрізків, ліміт байтів у польоті за BDP, мінімум ~64 КБ на пачку, послідовні знімки кількох інстансів, конфігурація шумних таблиць, перенесення в архів за предикатом, перехоплення `ExecuteUpdate`/`ExecuteDelete` у `WriteRouter` (він лише `SaveChangesInterceptor`), клас «масова» для імпорту, продовження знімка після перезапуску клієнта, відновлення з бекапу в самому додатку, тести лічильника 2³¹ і ліміту відрізків.

Батько архівного рядка (11.6) і порожня таблиця в handshake (6.2) реалізовані за дизайном і покриті тестами `ArchiveParentTests` і `EmptyReplicaTests`.