using Replication;
using Replication.Client;
using Replication.Net;

namespace Sample.Lab;

public sealed record ScenarioStep(string Title, string Explain, Func<Lab, Task> Do);

/// <summary>A scenario from the demo page (demo/sync-demo.html), run against the real core.</summary>
/// <param name="Verify">Checks the outcome after the last step; throws <see cref="ScenarioCheckException"/> when it is wrong.</param>
public sealed record Scenario(string Id, string Title, string Intro, Func<Lab, Task> Setup, IReadOnlyList<ScenarioStep> Steps, Func<Lab, Task>? Verify = null)
{
    public override string ToString() => Title;
}

public sealed class ScenarioCheckException(string message) : Exception(message);

public static class Scenarios
{
    public static readonly IReadOnlyList<Scenario> All =
    [
        new Scenario("free", "Вільний режим",
            "Обидва клієнти синхронні. Вимикайте зв'язок, редагуйте записи, запускайте автоматику на власнику. Позначка в колонці «черга»: дія чекає відправки або вже в дорозі.",
            BothSynced, []),

        new Scenario("offline", "Офлайн-черга і схлопування",
            "Сто змін одного запису передаються один раз. Черга зберігає тільки «що змінено», значення агент читає з репліки в момент відправки.",
            BothSynced,
            [
                new ScenarioStep("Вимкнути зв'язок Клієнта 1", "Дані лишаються доступні з локальної репліки. Статус показує «Нема зв'язку з …».",
                    l =>
                    {
                        l.C1.Link = false;
                        return Task.CompletedTask;
                    }),
                new ScenarioStep("Чотири правки «Степлера»", "У _sync_outbox один рядок: патч з колонками Price, Name і ModifiedOn. Значень у черзі нема.",
                    async l =>
                    {
                        foreach (long p in new long[] { 130, 140, 150 })
                            await l.C1.SetPriceAsync("Степлер", p);
                        await l.C1.RenameAsync("Степлер", "Степлер №10");
                    }),
                new ScenarioStep("Увімкнути зв'язок", "Перше повідомлення Start несе схему, instance_id і курсори, власник одразу відповідає першою пачкою, і клієнт паралельно починає Apply. Apply везе одну дію з останніми значеннями: 150 і «Степлер №10». Клієнт 2 отримує зміну потоком.",
                    l =>
                    {
                        l.C1.Link = true;
                        return Task.CompletedTask;
                    }),
            ],
            l =>
            {
                Check(Inspect.OwnerItem(l.Owner, "Степлер №10") is { Price: 150 }, "на власнику нема «Степлер №10» з ціною 150");
                return Task.CompletedTask;
            }),

        new Scenario("conflict", "Конфлікт у колонках",
            "Клієнт шле тільки змінені колонки. У тій самій колонці виграє той, хто останнім доїхав до власника.",
            BothSynced,
            [
                new ScenarioStep("Клієнт 1 офлайн змінює ціну паперу", "Патч з колонкою Price чекає в черзі.",
                    async l =>
                    {
                        l.C1.Link = false;
                        await l.C1.SetPriceAsync("Папір A4", 260);
                    }),
                new ScenarioStep("Автоматика змінює статус і ціну паперу", "Статус це інша колонка, ціна та сама. Клієнт 2 бачить обидві зміни автоматики.",
                    async l =>
                    {
                        await l.Owner.SetStatusAsync("Папір A4", "архів");
                        await l.Owner.SetPriceAsync("Папір A4", 199);
                    }),
                new ScenarioStep("Увімкнути зв'язок Клієнта 1", "Рядок з власника приїжджає, але Price у черзі, тому клієнт оновлює тільки статус. Потім Apply доставляє ціну 260: вона доїхала останньою і виграє, статус «архів» лишається.",
                    l =>
                    {
                        l.C1.Link = true;
                        return Task.CompletedTask;
                    }),
                new ScenarioStep("Автоматика знову змінює ціну", "Наступний прохід автоматики перезаписує правку клієнта. Так прийнято в дизайні: ручних правок на власнику нема.",
                    l => l.Owner.SetPriceAsync("Папір A4", 205)),
            ],
            l =>
            {
                Check(Inspect.OwnerItem(l.Owner, "Папір A4") is { Price: 205, Status: "архів" }, "папір: очікував ціну 205 і статус «архів»");
                return Task.CompletedTask;
            }),

        new Scenario("delete", "Видалення виграє",
            "Видалення сильніше за будь-яку правку, з будь-якого боку.",
            BothSynced,
            [
                new ScenarioStep("Клієнт 1 офлайн править «Маркери» і видаляє «Скотч»", "Видалений локально запис зникає одразу. У черзі патч і видалення.",
                    async l =>
                    {
                        l.C1.Link = false;
                        await l.C1.RenameAsync("Маркери", "Маркери (12 шт)");
                        await l.C1.DeleteItemAsync("Скотч");
                    }),
                new ScenarioStep("Автоматика видаляє «Маркери» і змінює ціну «Скотчу»", "На власнику з'являється tombstone Маркерів.",
                    async l =>
                    {
                        await l.Owner.DeleteItemAsync("Маркери");
                        await l.Owner.SetPriceAsync("Скотч", 45);
                    }),
                new ScenarioStep("Увімкнути зв'язок", "Tombstone Маркерів прибирає запис і правку з черги, користувач бачить повідомлення. Якщо першим доїде Apply, власник відповість Ignored, і клієнт так само прибере запис. Рядок «Скотчу» з новою ціною не вставляється, бо в черзі видалення, а Apply видаляє його на власнику.",
                    l =>
                    {
                        l.C1.Link = true;
                        return Task.CompletedTask;
                    }),
            ],
            l =>
            {
                Gone(l, "Маркери");
                Gone(l, "Маркери (12 шт)");
                Gone(l, "Скотч");
                Check(HasNote(l.C1, "втрачено"), "Клієнт 1 не отримав повідомлення про втрачену правку");
                return Task.CompletedTask;
            }),

        new Scenario("fk", "Ланцюжок FK і видалений батько",
            "Офлайн створений ланцюжок доїжджає цілісно, а запис під видаленим батьком відкидається.",
            BothSynced,
            [
                new ScenarioStep("Клієнт 1 офлайн створює категорію «Новинки», товар «Дрон» у ній і товар «Сканер» в «Архіві»", "Три створення в черзі, новим рядкам SyncVersion = 0.",
                    async l =>
                    {
                        l.C1.Link = false;
                        await l.C1.CreateCategoryAsync("Новинки");
                        await l.C1.CreateItemAsync("Дрон", 4200, "новий", "Новинки");
                        await l.C1.CreateItemAsync("Сканер", 1500, "новий", "Архів");
                    }),
                new ScenarioStep("Автоматика видаляє категорію «Архів»", "Каскад FK на власнику видаляє і товари категорії. Тригер дає tombstone кожному.",
                    l => l.Owner.DeleteCategoryAsync("Архів")),
                new ScenarioStep("Увімкнути зв'язок", "Пачка Apply закрита по залежностях: «Новинки» і «Дрон» застосовуються разом. «Сканер» отримує Rejected: parent deleted. Клієнт прибирає його з репліки і показує в повідомленнях.",
                    l =>
                    {
                        l.C1.Link = true;
                        return Task.CompletedTask;
                    }),
            ],
            l =>
            {
                Check(Inspect.OwnerHas(l.Owner, "Дрон"), "«Дрон» не доїхав до власника");
                Gone(l, "Сканер");
                Gone(l, "Факс");
                Check(HasNote(l.C1, "не збережено"), "Клієнт 1 не отримав повідомлення про відкинутий «Сканер»");
                return Task.CompletedTask;
            }),

        new Scenario("lost", "Втрачена відповідь Apply",
            "Повторна відправка безпечна: власник пропускає дії з seq ≤ applied_seq.",
            BothSynced,
            [
                new ScenarioStep("Загубити наступну відповідь і змінити ціну «Палети»", "Власник застосує дію і підніме applied_seq, але ApplyReply не дійде. Рядок черги лишається з sent = 1. Агент шле ту саму дію з тим самим seq, власник відповідає Skipped, і версія вдруге не росте.",
                    async l =>
                    {
                        l.C1.Agent.LoseNextApplyReply = true;
                        await l.C1.SetPriceAsync("Палета", 470);
                    }),
            ],
            l =>
            {
                Check(Inspect.OwnerItem(l.Owner, "Палета") is { Price: 470 }, "на власнику ціна «Палети» не 470");
                return Task.CompletedTask;
            }),

        new Scenario("inflight", "Правка, дія якої в дорозі",
            "Якщо запис змінили, поки його дія летить до власника, правка не губиться.",
            async l =>
            {
                await BothSynced(l);
                l.C1.Agent.Network.Latency = TimeSpan.FromMilliseconds(1500);
            },
            [
                new ScenarioStep("Змінити ціну «Стрейч-плівки», а поки Apply летить, змінити назву", "Затримка Клієнта 1 тут 1,5 с в кожен бік. Рядок черги з sent = 1 видаляється, вставляється новий без seq з колонками Price, Name. Відповідь на першу відправку прибирає тільки seq ≤ applied_up_to_seq, нова правка їде наступною пачкою.",
                    async l =>
                    {
                        await l.C1.SetPriceAsync("Стрейч-плівка", 333);
                        l.When(() => l.C1.Replication.Store.Entries(l.C1.Instance).Any(e => e.Sent == OutboxSendState.InFlight),
                            () => l.C1.RenameAsync("Стрейч-плівка", "Стрейч-плівка 500 мм"));
                    }),
            ],
            l =>
            {
                Check(Inspect.OwnerItem(l.Owner, "Стрейч-плівка 500 мм") is { Price: 333 }, "на власнику нема «Стрейч-плівка 500 мм» з ціною 333");
                return Task.CompletedTask;
            }),

        new Scenario("catchup", "Досинхронізація з обривом",
            "Після перерви найсвіжіші дані з'являються першими, а обрив не змушує тягнути отримане знову.",
            async l =>
            {
                l.Owner.Store.Options.CatchupBatchRows = 3;
                await BothSynced(l);
            },
            [
                new ScenarioStep("Клієнт 2 офлайн, автоматика робить 17 змін", "Курсори Клієнта 2 відстали від голови власника.",
                    async l =>
                    {
                        l.C2.Link = false;
                        await l.Owner.BurstAsync();
                    }),
                new ScenarioStep("Увімкнути зв'язок Клієнта 2 (обрив станеться сам)", "Власник щоразу бере поточну голову і шле найвищі невідправлені версії, від нових до старих, по 3 записи; потік до Клієнта 2 тут сповільнено. Спершу відкриті «Товари» разом з «Категоріями», на які вони посилаються, «Журнал» наприкінці. Посеред досинхронізації зв'язок рветься: у _sync_ranges лишається отриманий відрізок над курсором.",
                    l =>
                    {
                        l.Owner.Store.Options.Faults.DelayStream(l.C2.Replication.ClientId, TimeSpan.FromMilliseconds(400), TimeSpan.FromSeconds(30));
                        l.C2.Link = true;
                        l.When(() => l.C2.Agent.CursorOf("Item").Ranges.Count > 0,
                            () =>
                            {
                                l.C2.Link = false;
                                return Task.CompletedTask;
                            });
                        return Task.CompletedTask;
                    }),
                new ScenarioStep("Автоматика змінює ще два товари", "Нові версії більші за голову, яку бачив власник.",
                    async l =>
                    {
                        await l.Owner.SetPriceAsync("Степлер", 135);
                        await l.Owner.SetStatusAsync("Палета", "архів");
                    }),
                new ScenarioStep("Увімкнути зв'язок знову", "Start передає курсор і відрізки. Власник спершу шле нові зміни з верхньої прогалини, потім продовжує ту саму прогалину під відрізком. Уже отримане повторно не їде.",
                    l =>
                    {
                        l.C2.Link = true;
                        return Task.CompletedTask;
                    }),
            ]),

        new Scenario("flow", "Зміни швидші за пачку",
            "Поки клієнт досинхронізується, автоматика створює більше змін, ніж уміщує одна пачка. Найновіше однаково їде першим, а курсор плюс відрізки не дають тягнути отримане знову. Ліміту відрізків нема: статус чесно каже, що канал не встигає.",
            async l =>
            {
                l.Owner.Store.Options.CatchupBatchRows = 3;
                await BothSynced(l);
            },
            [
                new ScenarioStep("Клієнт 2 офлайн, автоматика робить 17 змін", "Курсор «Товарів» у Клієнта 2 відстав, відрізків ще нема.",
                    async l =>
                    {
                        l.C2.Link = false;
                        await l.Owner.BurstAsync();
                    }),
                new ScenarioStep("Увімкнути зв'язок і запустити потік: 4 зміни в «Товарах» кожні 0,4 с", "Пачка вміщує 3 записи, а потік дає більше. Перед кожною пачкою власник бере свіжу голову і шле верхню прогалину, тому в _sync_ranges з'являються нові відрізки. Залишок росте, і коли він не спадає кілька секунд поспіль (у дизайні 3 хв), статус змінюється на «Канал не встигає».",
                    l =>
                    {
                        l.Owner.Store.Options.Faults.DelayStream(l.C2.Replication.ClientId, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(16));
                        l.C2.Link = true;
                        for (int i = 1; i <= 25; i++)
                            l.Later(TimeSpan.FromMilliseconds(400 * i), () => l.Owner.FlowRoundAsync(4));
                        return Task.CompletedTask;
                    }),
                new ScenarioStep("Обірвати зв'язок посеред досинхронізації", "Потік на власнику триває. Клієнт зберіг курсор і всі відрізки в тій самій транзакції, що й рядки.",
                    l =>
                    {
                        l.C2.Link = false;
                        return Task.CompletedTask;
                    }),
                new ScenarioStep("Відновити зв'язок", "Start передає курсор і відрізки. Власник продовжує з верхньої прогалини, нічого з уже отриманого не їде вдруге. Коли потік зупиниться, пачки дотягнуть прогалини донизу, відрізки зіллються з курсором, і прийде TableSynced.",
                    l =>
                    {
                        l.C2.Link = true;
                        return Task.CompletedTask;
                    }),
            ]),

        new Scenario("lag", "Відставання понад 30 днів",
            "Tombstones чистяться після підтвердження всіма клієнтами або за терміном. Клієнт, що відстав, бере знімок і не губить черги.",
            async l =>
            {
                l.Owner.Store.Options.SnapshotChunkBytes = 4096;
                await BothSynced(l);
            },
            [
                new ScenarioStep("Клієнт 2 офлайн змінює ціну «Палети», автоматика видаляє два товари", "Клієнт 1 підтвердить tombstones, але очищення їх не видалить: floor = MIN(acked_version), а Клієнт 2 їх ще не отримав.",
                    async l =>
                    {
                        l.C2.Link = false;
                        await l.C2.SetPriceAsync("Палета", 999);
                        await l.Owner.DeleteItemAsync("Скотч");
                        await l.Owner.DeleteItemAsync("Маркери");
                        l.Later(TimeSpan.FromSeconds(1.5), async () => await l.Owner.PurgeAsync());
                    }),
                new ScenarioStep("Перемотати на 31 день уперед", "Клієнт 2 не з'являвся понад 30 днів, тому його рядок зникає з _sync_clients. Тепер floor рахується лише по Клієнту 1, tombstones видаляються, і purged_version стає вищою за курсор Клієнта 2.",
                    async l =>
                    {
                        l.Owner.AdvanceClock(TimeSpan.FromDays(31));
                        await l.Owner.PurgeAsync();
                    }),
                new ScenarioStep("Увімкнути зв'язок Клієнта 2", "Start: курсор < purged_version, тому власник відповідає SnapshotRequired і закриває потік. Агент спершу відправляє чергу (ціна «Палети»), потім завантажує знімок шматками по 4 КБ. Посеред завантаження користувач перейменовує папір у старій репліці: дія стає в чергу і переноситься в нову репліку разом зі значенням.",
                    l =>
                    {
                        l.C2.Agent.Network.DownBitsPerSecond = 200_000;
                        l.C2.Link = true;
                        l.When(() => l.C2.Agent.GetStatus().SnapshotProgress > 0.2, async () =>
                        {
                            await l.C2.RenameAsync("Папір A4", "Папір A4 (500 арк.)");
                            l.C2.Agent.Network.DownBitsPerSecond = 0;
                        });
                        return Task.CompletedTask;
                    }),
            ],
            l =>
            {
                Gone(l, "Скотч");
                Gone(l, "Маркери");
                Check(Inspect.OwnerItem(l.Owner, "Палета") is { Price: 999 }, "ціна «Палети» з черги Клієнта 2 не доїхала");
                Check(Inspect.OwnerHas(l.Owner, "Папір A4 (500 арк.)"), "перейменування з часу завантаження знімка загубилось");
                return Task.CompletedTask;
            }),

        new Scenario("bulk", "Масове видалення за умовою",
            "Масове видалення їде однією дією: умова і версія V, до якої репліка повна. Власник видаляє тільки те, що клієнт бачив, а правка користувача не чекає за масовою дією.",
            BothSynced,
            [
                new ScenarioStep("Клієнт 1 офлайн видаляє всі товари зі статусом «архів»", "Записи зникли з репліки, а в чергу пішла одна масова дія: Status = 'архів' і SyncVersion ≤ V, де V це курсор «Товарів». Ключі не передаються.",
                    async l =>
                    {
                        l.C1.Link = false;
                        await l.C1.DeleteWhereStatusAsync("архів");
                    }),
                new ScenarioStep("Клієнт 1 міняє ціну «Степлера»", "Ця правка інтерактивна. Вона створена пізніше за масову дію, але в пачці піде першою: seq дається при відправці.",
                    l => l.C1.SetPriceAsync("Степлер", 135)),
                new ScenarioStep("Автоматика створює «Архівну шафу» зі статусом «архів»", "Цей запис підпадає під умову, але користувач його не бачив, і його SyncVersion більша за V.",
                    l => l.Owner.NewItemAsync("Архівна шафа", "архів", "Склад")),
                new ScenarioStep("Увімкнути зв'язок", "Apply везе правку ціни і масове видалення. Власник виконує DELETE … WHERE SyncVersion ≤ V AND Status = 'архів': три записи, які бачив користувач, видалено. «Архівна шафа» новіша за V, тому лишається і повертається як змінена. Клієнт показує, що її не видалено, і вона приїжджає потоком.",
                    l =>
                    {
                        l.C1.Link = true;
                        return Task.CompletedTask;
                    }),
            ],
            l =>
            {
                Gone(l, "Старий принтер");
                Gone(l, "Каталог 2019");
                Check(Inspect.OwnerHas(l.Owner, "Архівна шафа") && Inspect.ClientHas(l.C1, "Архівна шафа"), "«Архівна шафа» мала лишитись і приїхати до Клієнта 1");
                Check(Inspect.OwnerItem(l.Owner, "Степлер") is { Price: 135 }, "правка ціни «Степлера» не доїхала");
                Check(HasNote(l.C1, "не видалено"), "Клієнт 1 не отримав повідомлення про невидалений запис");
                return Task.CompletedTask;
            }),

        new Scenario("snapshot", "Новий клієнт: знімок",
            "Клієнт без репліки отримує знімок шматками і продовжує його після обриву з того ж зміщення.",
            async l =>
            {
                l.Owner.Store.Options.SnapshotChunkBytes = 4096;
                await l.SyncNowAsync(l.C1);
            },
            [
                new ScenarioStep("Увімкнути зв'язок Клієнта 2", "Репліки нема, тому на Start власник відповідає SnapshotRequired з розміром знімка. Файл малий, тому клієнт бере файл, а не порожню репліку. Власник робить VACUUM INTO, реєструє клієнта з acked_version = V і віддає файл шматками по 4 КБ (канал Клієнта 2 тут 100 кбіт/с). Поки файл їде, автоматика змінює «Степлер». На третині файлу зв'язок рветься, а після відновлення завантаження продовжується з того ж зміщення за snapshot_id. Після підміни Subscribe довозить зміну «Степлера», новішу за V.",
                    l =>
                    {
                        l.C2.Agent.Network.DownBitsPerSecond = 100_000;
                        l.C2.Link = true;
                        l.When(() => l.C2.Agent.GetStatus().SnapshotProgress > 0.15, () => l.Owner.SetPriceAsync("Степлер", 175));
                        l.When(() => l.C2.Agent.GetStatus().SnapshotProgress > 0.35, () =>
                        {
                            l.C2.Link = false;
                            l.Later(TimeSpan.FromSeconds(1.5), () =>
                            {
                                l.C2.Agent.Network.DownBitsPerSecond = 0;
                                l.C2.Link = true;
                                return Task.CompletedTask;
                            });
                            return Task.CompletedTask;
                        });
                        return Task.CompletedTask;
                    }),
            ],
            l =>
            {
                Check(Inspect.ClientHas(l.C2, "Степлер"), "у Клієнта 2 нема «Степлера»");
                return Task.CompletedTask;
            }),

        new Scenario("archive", "Перенесення в архів",
            "Користувач забирає частину даних собі й звільняє місце на власнику. Рядки в БД клієнта спершу отримують InstanceId = X:archive, синк їх більше не чіпає, і тільки потім власнику йде умовне видалення.",
            async l =>
            {
                await BothSynced(l);
                l.C1.Agent.Network.Latency = TimeSpan.FromMilliseconds(700);
            },
            [
                new ScenarioStep("Клієнт 1 переносить в архів категорію «Архів» і товар «Каталог 2019»", "Замикання по FK додає дочірні «Старий принтер» і «Факс». Набір отримує InstanceId = X:archive однією транзакцією. У чергу йде одна дія: Id ∈ набір і SyncVersion ≤ V. Поки Apply летить, автоматика змінює ціну «Факсу»; рядок в архіві, тому ця зміна проходить повз. Власник видаляє рядки з SyncVersion ≤ V: «Факс» новіший і лишається, а з ним його батько «Архів». Клієнт повертає обидва з архіву в репліку, «Факс» іде в NeedFull, і клієнт повторює перенесення.",
                    async l =>
                    {
                        await l.C1.ArchiveAsync(("Category", "Архів"), ("Item", "Каталог 2019"));
                        l.When(() => l.C1.Replication.Store.Entries(l.C1.Instance).Any(e => e.Kind == OutboxKind.Archive && e.Sent == OutboxSendState.InFlight),
                            () => l.Owner.SetPriceAsync("Факс", 320));
                    }),
                new ScenarioStep("Подивитись на результат", "Tombstones прибрали записи з репліки Клієнта 2. У Клієнта 1 вони лишились рядками з InstanceId = X:archive: це тепер єдина копія. Власник повертає звільнені сторінки на диск через PRAGMA incremental_vacuum.",
                    l =>
                    {
                        l.Owner.Store.IncrementalVacuum(64);
                        return Task.CompletedTask;
                    }),
            ],
            l =>
            {
                foreach (string n in new[] { "Старий принтер", "Факс", "Каталог 2019" })
                {
                    Gone(l, n);
                    Check(Inspect.ClientHas(l.C1, n, archive: true), $"«{n}» нема в архіві Клієнта 1");
                }
                return Task.CompletedTask;
            }),

        new Scenario("columns", "Оновлення по колонках",
            "Власник шле не весь рядок, а тільки колонки, змінені після версії, яку вже мають усі активні клієнти (SyncBase і маска SyncMask). Хто довго був поза зв'язком, отримує рядок цілком.",
            BothSynced,
            [
                new ScenarioStep("Автоматика змінює ціну «Степлера»", "Попередню версію рядка мають обидва клієнти (floor ≥ стара SyncVersion), тому тригер ставить SyncBase = стара версія, маска = Price, ModifiedOn. Онлайн-пачка везе обом тільки Id, версію і ці колонки.",
                    l => l.Owner.SetPriceAsync("Степлер", 130)),
                new ScenarioStep("Клієнт 2 офлайн, автоматика змінює ціну, а потім статус", "Клієнт 2 активний (був на зв'язку менше доби тому), але його курсор більше не росте, тому floor стоїть, і друга зміна вже не зсуває базу, а накопичує маску. Клієнт 1 отримує всі колонки маски, хоча частина в нього вже є.",
                    async l =>
                    {
                        l.C2.Link = false;
                        await l.Owner.SetPriceAsync("Степлер", 140);
                        long head = l.Owner.Head();
                        await Lab.WaitAsync(() => !l.C1.Link || l.C1.Agent.CursorOf("Item").Cursor >= head, TimeSpan.FromSeconds(15), "Клієнт 1 не отримав нову ціну «Степлера»");
                        await l.Owner.SetStatusAsync("Степлер", "архів");
                    }),
                new ScenarioStep("Минає доба: Клієнт 2 випадає з вікна активності", "Клієнт 2 не був на зв'язку довше за вікно активності (24 год), тому floor рахується тільки за курсором Клієнта 1.",
                    l =>
                    {
                        l.Owner.AdvanceClock(TimeSpan.FromHours(25));
                        return Task.CompletedTask;
                    }),
                new ScenarioStep("Автоматика перейменовує «Степлер»", "Тепер floor дорівнює курсору Клієнта 1, тому база зсувається вище за курсор Клієнта 2, а маска = Name, ModifiedOn. Клієнт 1 отримує тільки ці колонки.",
                    l => l.Owner.RenameAsync("Степлер", "Степлер Pro")),
                new ScenarioStep("Клієнт 2 повертається", "Курсор Клієнта 2 нижчий за SyncBase «Степлера»: стану рядка на базу в нього нема, тому досинхронізація везе рядок цілком.",
                    l =>
                    {
                        l.C2.Link = true;
                        return Task.CompletedTask;
                    }),
            ]),

        new Scenario("echo", "Без відлуння",
            "Власник не повертає клієнту його власні зміни: рядок пам'ятає SyncOrigin, а нову версію клієнт дізнається з ApplyReply. Виняток: правка поверх чужої зміни, якої клієнт ще не бачив.",
            BothSynced,
            [
                new ScenarioStep("Клієнт 1 змінює ціну «Степлера»", "Попередню версію рядка Клієнт 1 бачив, тому Apply ставить SyncOrigin = Клієнт 1. ApplyReply повертає нову версію. Клієнт 2 отримує зміну, а для Клієнта 1 рядок пропущено: приходить тільки Head, і курсор проходить через цю версію.",
                    l => l.C1.SetPriceAsync("Степлер", 155)),
                new ScenarioStep("Клієнт 1 без зв'язку, Клієнт 2 міняє статус «Степлера»", "Тепер SyncOrigin = Клієнт 2. Клієнт 1 цієї зміни не бачив.",
                    async l =>
                    {
                        l.C1.Link = false;
                        await l.C2.SetStatusAsync("Степлер", "архів");
                    }),
                new ScenarioStep("Клієнт 1 офлайн перейменовує «Степлер» і повертається", "Курсор Клієнта 1 на власнику нижчий за версію зі зміною статусу, тому Apply лишає SyncOrigin = NULL і version не повертає. Клієнт 1 отримує рядок назад і бачить і свою назву, і чужий статус.",
                    async l =>
                    {
                        await l.C1.RenameAsync("Степлер", "Степлер Max");
                        l.Later(TimeSpan.FromSeconds(1), () =>
                        {
                            l.C1.Link = true;
                            return Task.CompletedTask;
                        });
                    }),
                new ScenarioStep("Клієнт 2 видаляє «Скотч»", "Tombstone має origin = Клієнт 2: Клієнт 1 отримує видалення, а Клієнту 2, який уже прибрав рядок сам, воно не повертається.",
                    l => l.C2.DeleteItemAsync("Скотч")),
            ],
            l =>
            {
                Check(Inspect.OwnerItem(l.Owner, "Степлер Max") is { Status: "архів" }, "на власнику мав бути «Степлер Max» зі статусом «архів»");
                Gone(l, "Скотч");
                return Task.CompletedTask;
            }),

        new Scenario("delwins", "Видалення виграє завжди",
            "Видалення перемагає будь-яку правку, хоч би в якому порядку вони дійшли: запис зникає на власнику і в усіх репліках, а правка відкидається з повідомленням (дизайн 9.1).",
            BothSynced,
            [
                new ScenarioStep("Клієнт 1 видаляє «Палету», за мить Клієнт 2 змінює її ціну", "Видалення доходить до власника першим. Потік до Клієнта 2 тут затримано, тому ApplyReply обганяє tombstone: власник відповідає Ignored, і Клієнт 2 одразу прибирає рядок у себе з повідомленням. Пізній tombstone вже нічого не міняє.",
                    async l =>
                    {
                        l.Owner.Store.Options.Faults.DelayStream(l.C2.Replication.ClientId, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2.5));
                        await l.C1.DeleteItemAsync("Палета");
                        l.Later(TimeSpan.FromMilliseconds(500), () => l.C2.SetPriceAsync("Палета", 999));
                    }),
                new ScenarioStep("Клієнт 1 офлайн видаляє «Скотч», Клієнт 2 його перейменовує", "Правка Клієнта 2 доходить до власника першою. Клієнт 1 після повернення отримує новішу версію рядка, але не вставляє її, бо в черзі видалення. Apply видаляє рядок без перевірки версії, і tombstone прибирає «Скотч» у Клієнта 2.",
                    async l =>
                    {
                        l.C1.Link = false;
                        await l.C1.DeleteItemAsync("Скотч");
                        await l.C2.RenameAsync("Скотч", "Скотч широкий");
                        l.Later(TimeSpan.FromSeconds(1.5), () =>
                        {
                            l.C1.Link = true;
                            return Task.CompletedTask;
                        });
                    }),
                new ScenarioStep("Клієнт 2 офлайн змінює «Маркери», Клієнт 1 їх видаляє", "Тут tombstone доходить раніше за Apply: досинхронізація йде до першої відправки черги. Tombstone прибирає рядок і правку з черги, користувач бачить повідомлення.",
                    async l =>
                    {
                        l.C2.Link = false;
                        await l.C2.SetPriceAsync("Маркери", 99);
                        await l.C1.DeleteItemAsync("Маркери");
                        l.Later(TimeSpan.FromSeconds(1.5), () =>
                        {
                            l.C2.Link = true;
                            return Task.CompletedTask;
                        });
                    }),
                new ScenarioStep("Пізня пачка: автоматика змінює «Степлер», Клієнт 1 одразу його видаляє", "Потік до Клієнта 1 затримано: пачка з новою ціною «Степлера» вийшла раніше за видалення, а доходить пізніше за ApplyReply. Рядок черги з видаленням лишається з sent = 2, доки курсор таблиці не пройде версію tombstone, тож пізня пачка рядок не повертає.",
                    async l =>
                    {
                        l.Owner.Store.Options.Faults.DelayStream(l.C1.Replication.ClientId, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
                        await l.Owner.SetPriceAsync("Степлер", 175);
                        await Task.Delay(400);
                        await l.C1.DeleteItemAsync("Степлер");
                    }),
            ],
            l =>
            {
                foreach (string n in new[] { "Палета", "Скотч", "Скотч широкий", "Маркери", "Степлер" })
                    Gone(l, n);
                Check(HasNote(l.C2, "втрачено"), "Клієнт 2 не отримав повідомлення про втрачену правку");
                return Task.CompletedTask;
            }),

        new Scenario("schema", "Несумісна версія",
            "Перше повідомлення Start несе версію схеми. При розбіжності власник відповідає SchemaMismatch і закриває потік, синк зупиняється, а черга зберігається.",
            BothSynced,
            [
                new ScenarioStep("Клієнт 1 офлайн змінює ціну і оновлюється до нової схеми", "Дія в черзі.",
                    async l =>
                    {
                        l.C1.Link = false;
                        await l.C1.SetPriceAsync("Скотч", 55);
                        l.C1.Agent.SetSchemaVersion(2);
                    }),
                new ScenarioStep("Увімкнути зв'язок", "Власник відповідає SchemaMismatch. Статус: «Оновіть додаток на інстансі або клієнті». Черга не відправляється і не губиться.",
                    async l =>
                    {
                        l.C1.Link = true;
                        await Lab.WaitAsync(() => l.C1.Agent.GetStatus().State == AgentState.SchemaMismatch, TimeSpan.FromSeconds(15), "нема SchemaMismatch");
                    }),
                new ScenarioStep("Повернути схему 1", "Обидві сторони знову на одній версії: агент перепідключається, і черга доїжджає.",
                    l =>
                    {
                        l.C1.Agent.SetSchemaVersion(1);
                        return Task.CompletedTask;
                    }),
            ],
            l =>
            {
                Check(Inspect.OwnerItem(l.Owner, "Скотч") is { Price: 55 }, "ціна «Скотчу» з черги не доїхала після повернення схеми");
                return Task.CompletedTask;
            }),
    ];

    private static void Check(bool ok, string what)
    {
        if (!ok)
            throw new ScenarioCheckException(what);
    }

    private static async Task BothSynced(Lab l)
    {
        await l.SyncNowAsync(l.C1);
        await l.SyncNowAsync(l.C2);
    }

    private static void Gone(Lab l, string name)
    {
        Check(!Inspect.OwnerHas(l.Owner, name), $"«{name}» лишився на власнику");
        foreach (ClientNode c in l.Clients.Where(c => c.Link))
            Check(!Inspect.ClientHas(c, name), $"«{name}» лишився в репліці {c.Label}");
    }

    private static bool HasNote(ClientNode c, string fragment) =>
        c.Replication.Store.Notes().Any(n => n.Text.Contains(fragment, StringComparison.Ordinal));

    public static Scenario Find(string id) => All.First(s => s.Id == id);
}

/// <summary>Runs a scenario step by step the way the app and the tests do.</summary>
public sealed class ScenarioRunner(Lab lab, Scenario scenario)
{
    public Lab Lab { get; } = lab;
    public Scenario Scenario { get; } = scenario;
    public int Step { get; private set; }
    public bool Done => Step >= Scenario.Steps.Count;

    public async Task SetupAsync()
    {
        Lab.Say($"сценарій «{Scenario.Title}»: старт", SyncLogLevel.Ok);
        await Scenario.Setup(Lab);
    }

    public async Task NextAsync()
    {
        if (Done)
            return;
        ScenarioStep s = Scenario.Steps[Step++];
        Lab.Say($"— крок {Step}: {s.Title}", SyncLogLevel.Ok);
        await s.Do(Lab);
    }

    /// <summary>Waits for the system to settle, then checks the outcome and that every connected replica equals the owner.</summary>
    public async Task<List<string>> VerifyAsync(TimeSpan? timeout = null)
    {
        await Lab.SettleAsync(timeout);
        var problems = new List<string>();
        if (Scenario.Verify is { } v)
        {
            try
            {
                await v(Lab);
            }
            catch (ScenarioCheckException e)
            {
                problems.Add(e.Message);
            }
        }

        foreach (ClientNode c in Lab.Clients.Where(c => c.Link && c.Agent.GetStatus().State == AgentState.Online))
            problems.AddRange(Inspect.Diff(Lab.Owner, c).Select(d => $"{c.Label}: {d}"));
        Lab.Say(problems.Count == 0 ? "перевірка: репліки дорівнюють власнику, очікуваний результат є" : "перевірка: " + string.Join("; ", problems),
            problems.Count == 0 ? SyncLogLevel.Ok : SyncLogLevel.Bad);
        return problems;
    }
}
