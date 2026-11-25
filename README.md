# Supplier Management — .NET 8, Kafka ve SQL Server

[![CI](https://github.com/cakmakbm/SupplierManagement/actions/workflows/ci.yml/badge.svg)](https://github.com/cakmakbm/SupplierManagement/actions/workflows/ci.yml)

Bu depo, bir ürünün yazma servisinde oluşturulup Kafka eventi aracılığıyla ayrı bir katalog read-model'ine aktarılmasını gösteren küçük bir mikroservis çalışmasıdır.

> **Kapsam:** Transactional Outbox ile güvenilir event yayımlama, idempotent consumer, hata durumunda mesaj kaybetmeyen manuel offset yönetimi, Serilog, health check, unit testler ve GitHub Actions CI vardır. Authentication, dead-letter topic ve retry policy henüz yoktur.

## Mimari

```text
POST /api/products
        │
        ▼
Supplier.ProductService ──► ProductDb (Products + OutboxMessages, tek transaction)
        │
        │ OutboxPublisherService (arka plan worker)
        │ ProductCreated
        ▼
Kafka: product-created
        │
        ▼
Catalog.ReadService ──────► CatalogDb
        │
        ▼
GET /api/catalog/products
```

- `Supplier.ProductService`: ürünün sahibi olan write-model API'sidir. Ürünü ve `ProductCreated` eventini aynı transaction'da `Products` ve `OutboxMessages` tablolarına yazar; arka plandaki `OutboxPublisherService` eventi Kafka'ya yayımlar.
- `Catalog.ReadService`: eventi bir `BackgroundService` ile tüketir ve sorgulamaya uygun ayrı read-model'i oluşturur.
- `Shared.Contracts`: servisler arasında paylaşılan `ProductCreated` sözleşmesini içerir.
- Her servis kendi veritabanına sahiptir; servisler birbirlerinin tablolarına erişmez.

## Kafka'nın görevi ve temel kavramlar

Kafka servisleri zaman ve çalışma biçimi açısından birbirinden ayırır. ProductService katalog servisini doğrudan çağırmaz.

- **Producer:** `ProductCreated` eventini Kafka'ya gönderen ProductService.
- **Consumer:** eventi okuyup read-model'e uygulayan CatalogService.
- **Topic:** eventlerin yayımlandığı akış; burada `product-created`.
- **Partition:** topic'i paralel ve sıralı alt akışlara böler. Sıralama yalnızca aynı partition içinde garantilidir.
- **Key:** partition seçimini etkiler. Bu projede `SupplierId` kullanılır; aynı tedarikçinin ürün eventleri aynı partition'a gider ve kendi aralarındaki sıra korunur. Çok büyük tek bir tedarikçi yükü dengesizleştirirse `ProductId` tercih edilebilir.
- **Offset:** consumer'ın partition içindeki okuma konumudur.
- **Consumer group:** aynı işi yapan consumer instance'larının partition'ları paylaşmasını sağlar. Grup adı `catalog-read-service`tir.

Consumer otomatik commit kullanmaz. Veritabanı işlemi tamamlandıktan sonra offset manuel commit edilir. `ProcessedEvents.EventId` birincil anahtarı ve `ProductId` kontrolü aynı event tekrar teslim edilirse duplicate kayıt oluşmasını engeller. Geçersiz JSON loglanır ve partition'ı sonsuza kadar kilitlememesi için offset'i commit edilir; ileride dead-letter topic eklenebilir.

**Hata durumunda neden `Seek` gerekir?** `Consume()` çağrısı consumer'ın bellekteki okuma konumunu bir sonraki mesaja taşır. İşleme başarısız olduğunda (örneğin veritabanına o an erişilemiyorsa) yalnızca commit etmemek yeterli değildir: bir sonraki `Consume()` sonraki mesajı döndürür, o mesaj başarıyla işlenip commit edildiğinde başarısız mesaj kalıcı olarak atlanır. Bu yüzden `ProductCreatedMessageHandler` hata durumunda `consumer.Seek(...)` ile aynı offset'e geri döner; mesaj, işlenene kadar tekrar denenir ve kaybolmaz.

## Çalıştırma

Gereksinimler: Docker Desktop ve Docker Compose.

Apple Silicon'da SQL Server 2022 image'ı yalnızca x86 olduğundan Docker Desktop'ta "Use Rosetta for x86_64/amd64 emulation" ayarı açık olmalıdır; aksi halde SQL Server container'ı segfault ile kapanır.

```bash
docker compose up --build
```

Servisler hazır olduğunda:

- ProductService Swagger: http://localhost:5001/swagger
- CatalogService Swagger: http://localhost:5002/swagger
- ProductService health: http://localhost:5001/health
- CatalogService health: http://localhost:5002/health
- Kafka dış bağlantı: `localhost:9092`
- Product SQL Server: `localhost:1433`
- Catalog SQL Server: `localhost:1434`

Sistemi ve volume'leri temizlemek için `docker compose down -v` kullanılabilir. Bu komut iki veritabanındaki verileri siler.

## Swagger ile örnek mesaj akışı

1. ProductService Swagger'da `POST /api/products` çağrısını açın.
2. Şu örnek gövdeyi gönderin:

```json
{
  "supplierId": "11111111-1111-1111-1111-111111111111",
  "name": "Endüstriyel Matkap",
  "sku": "DRILL-001",
  "price": 2499.90,
  "stock": 25
}
```

3. ProductService ürünü `ProductDb` içine kaydeder ve `product-created` topic'ine event gönderir.
4. CatalogService logunda aynı `EventId`, `ProductId` ve topic ile tüketim kaydı görünür.
5. CatalogService Swagger'da `GET /api/catalog/products` çağrısı ürünü döndürür.

Komut satırı alternatifi:

```bash
curl -X POST http://localhost:5001/api/products \
  -H 'Content-Type: application/json' \
  -d '{"supplierId":"11111111-1111-1111-1111-111111111111","name":"Endüstriyel Matkap","sku":"DRILL-001","price":2499.90,"stock":25}'

curl http://localhost:5002/api/catalog/products
```

## Transactional Outbox

Veritabanına yazıp ardından doğrudan Kafka'ya göndermek iki ayrı sisteme yazmak demektir (dual write): veritabanı kaydı başarılı olup Kafka gönderimi başarısız olursa ürün kaydolur fakat event kaybolur.

Bu yüzden ProductService **Transactional Outbox Pattern** kullanır:

1. `CreateProductHandler`, ürünü ve `ProductCreated` eventini tek `SaveChanges` çağrısıyla, yani tek veritabanı transaction'ında `Products` ve `OutboxMessages` tablolarına yazar. İkisi ya birlikte kaydolur ya hiç kaydolmaz.
2. `OutboxPublisherService`, `OutboxDispatcher` ile gönderilmemiş mesajları ekleme sırasına göre Kafka'ya yollar ve başarılı olanlara `ProcessedAt` yazar.
3. Kafka'ya erişilemezse mesaj bekleyen durumda kalır, `Attempts` ve `LastError` güncellenir ve sonraki turda tekrar denenir. Sıranın bozulmaması için ilk hatada o tur durur.

Teslimat **at-least-once**'dır: Kafka mesajı kabul ettikten sonra `ProcessedAt` kaydedilemeden süreç durursa mesaj aynı `EventId` ile tekrar yayımlanır. Consumer'daki `EventId` kontrolü bu tekrarı yok sayar.

Mevcut bir `ProductDb` volume'u outbox öncesinden kalmışsa `OutboxMessages` tablosu `EnsureCreated` ile eklenmez; `docker compose down -v` ile veritabanını sıfırlayın.

## API hata yanıtları

| Durum | HTTP |
|-------|------|
| Geçersiz gövde, örneğin boş veya `00000000-...` `supplierId` | 400 `ValidationProblemDetails` |
| Aynı tedarikçi için aynı SKU | 409 `ProblemDetails` |

## Testler

```bash
dotnet test SupplierManagement.slnx
```

`.slnx` çözüm formatı için .NET SDK 9.0.200 veya üstü gerekir; projeler `net8.0` hedefler ve testler .NET 8 runtime ister. Makinede .NET 8 runtime yoksa testler Docker ile çalıştırılabilir:

```bash
docker run --rm -v "$PWD":/src:ro mcr.microsoft.com/dotnet/sdk:8.0 sh -c \
  'cp -r /src /work && cd /work && dotnet test tests/Supplier.ProductService.Tests && dotnet test tests/Catalog.ReadService.Tests'
```

Testler in-memory SQLite ve NSubstitute ile Kafka istemcilerinin mock'larını kullanır; Docker veya Kafka gerekmez.

- **Supplier.ProductService.Tests:** gerçek ASP.NET Core pipeline'ı üzerinden `POST /api/products` (201, duplicate SKU için 409, boş `SupplierId` için 400), ürün ve outbox kaydının birlikte yazılması, veritabanı hatasında ikisinin de yazılmaması, duplicate SKU, boş `SupplierId` doğrulaması, outbox'ın sıralı yayımlanması, Kafka hatasında mesajın beklemede kalması ve tekrar denemede aynı `EventId`'nin kullanılması.
- **Catalog.ReadService.Tests:** yeni eventin uygulanıp commit edilmesi, aynı eventin iki kez gelmesi, veritabanı hatasında aynı offset'e `Seek` yapılıp commit edilmemesi, tekrar denemede mesajın bir kez uygulanması ve geçersiz JSON'un atlanması.

## CI

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) her push ve pull request'te çözümü derler, tüm testleri çalıştırır ve iki servisin Docker image'ının derlendiğini doğrular.

**Eventual consistency**, iki servis verisinin her an eşit olmak zorunda olmaması, ancak event işlendiğinde sonunda aynı iş gerçeğini yansıtmasıdır. POST başarılı olduktan hemen sonraki kısa sürede katalog GET sonucu ürünü henüz göstermeyebilir; consumer işlediğinde görünür hale gelir.

## Öğretici sadelik tercihleri

- İlk çalıştırmayı kolaylaştırmak için servisler `EnsureCreated` kullanır. Şema evrimi gereken gerçek projede EF Core migrations tercih edilmelidir.
- Kafka topic auto-create ile oluşur. Production ortamında partition, replication ve retention ayarları altyapı tarafından açıkça yönetilmelidir.
- Bu aşamada DLQ, retry policy ve authentication/TLS bilinçli olarak kapsam dışıdır.
- Outbox tablosu tek bir dispatcher için tasarlanmıştır. Birden fazla ProductService instance'ı aynı anda çalışırsa bir mesaj iki kez yayımlanabilir; consumer idempotency bunu tolere eder. Yüksek ölçekte satır kilitleme (`UPDLOCK, READPAST`) veya Debezium gibi CDC tabanlı bir çözüm tercih edilebilir.
