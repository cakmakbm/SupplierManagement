# Supplier Management — .NET 8, Kafka ve SQL Server

Bu depo, bir ürünün yazma servisinde oluşturulup Kafka eventi aracılığıyla ayrı bir katalog read-model'ine aktarılmasını gösteren küçük bir mikroservis çalışmasıdır.

> **Aşama 1 kapsamı:** Basit producer-consumer akışı, consumer idempotency, manuel offset commit, Serilog ve health check vardır. Transactional Outbox, authentication ve otomatik testler henüz yoktur.

## Mimari

```text
POST /api/products
        │
        ▼
Supplier.ProductService ──► ProductDb
        │
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

- `Supplier.ProductService`: ürünün sahibi olan write-model API'sidir. SQL Server'a kaydettikten sonra eventi yayımlar.
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

## Çalıştırma

Gereksinimler: Docker Desktop ve Docker Compose.

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

## Bilinen güvenilirlik boşluğu ve sonraki aşama

Bu ilk sürümde ProductService önce veritabanına yazar, sonra Kafka'ya gönderir. Veritabanı kaydı başarılı olup Kafka gönderimi başarısız olursa ürün kaydolur fakat event kaybolur. Tersine, gönderim sonucu belirsiz kalırsa event yeniden yayımlanabilir. Bunlar iki ayrı sistem üzerinde tek atomik transaction bulunmamasından kaynaklanır.

Sonraki aşamada **Transactional Outbox Pattern** eklenecektir: ürün ve event aynı veritabanı transaction'ında `Products` ile `OutboxMessages` tablolarına yazılır; ayrı worker gönderilmemiş kayıtları Kafka'ya yollar ve başarılı olanlara `ProcessedAt` yazar. Böylece veritabanı ile event üretimi arasındaki kayıp penceresi kapanır. Kafka teslimatı yine tekrar edebileceğinden consumer idempotency korunur.

**Eventual consistency**, iki servis verisinin her an eşit olmak zorunda olmaması, ancak event işlendiğinde sonunda aynı iş gerçeğini yansıtmasıdır. POST başarılı olduktan hemen sonraki kısa sürede katalog GET sonucu ürünü henüz göstermeyebilir; consumer işlediğinde görünür hale gelir.

## Öğretici sadelik tercihleri

- İlk çalıştırmayı kolaylaştırmak için servisler `EnsureCreated` kullanır. Şema evrimi gereken gerçek projede EF Core migrations tercih edilmelidir.
- Kafka topic auto-create ile oluşur. Production ortamında partition, replication ve retention ayarları altyapı tarafından açıkça yönetilmelidir.
- Bu aşamada DLQ, retry policy, authentication/TLS ve Outbox bilinçli olarak kapsam dışıdır.
