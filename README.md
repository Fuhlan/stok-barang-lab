# Capstone Project: A10 Penerimaan Stok Barang (`stok-barang-lab`)

Program Pelatihan Jarak Jauh (PJJ):  
**Implementasi dan Pengelolaan Message Broker untuk Arsitektur Microservices**  
Pusdiklat Keuangan Publik BPPK - Kementerian Keuangan RI bekerja sama dengan ADINESIA (September 2026)

---

## 1. Identitas & Kontribusi Tim

- **Kode Kasus**: `A10`
- **Judul Kasus**: Penerimaan Stok Barang (Halaman 19 Panduan Capstone)
- **Nama Tim / Peserta**:
  (Kelompok 4)
  - Fajar Irvandi
  - Danang Ajie Nugraha
  - Taufiq Mahendra
- **Pembagian Kontribusi**:
  - _Infrastruktur & Broker Topology_: Konfigurasi RabbitMQ (Direct Exchange `inventory`, DLX `inventory.dlx`, antrean `stock_updates`, DLQ `stock_updates.dlq`, dan Docker Compose).
  - _Database Design & Idempotency Engine_: Skema PostgreSQL (DDL `stok_barang`, `ledger_penerimaan`, `rejected_events`) dan transaksi atomik database (`BEGIN ... FOR UPDATE ... COMMIT/ROLLBACK`).
  - _Layanan Consumer (Worker)_: Implementasi manual acknowledgement, prefetch QoS, dead-letter reject routing, serta penanganan crash recovery.
  - _Layanan Ingress Producer (API & CLI)_: Endpoint HTTP REST Express (`/penerimaan`) dengan publisher confirm dan pengirim event CLI batch/sintetis.
  - _Automated Testing & Skenario Modular_: Pembuatan runner pengujian mandiri U1, U2, U3, U4 serta pengujian terpadu orchestrator dan pelaporan bukti uji.
  - _Technical Documentation & Presentation_: Pembuatan dokumentasi teknis dan presentasi hasil.

---

## 2. Deskripsi Kasus & Ruang Lingkup Masalah

### Karakteristik Masalah (Kasus A10)

Gudang fiktif mencatat barang yang diterima melalui event asinkron (`stock.received`). Pengiriman ulang event (disebabkan oleh _network retry_, _broker redelivery_, atau _duplicate publish_) berisiko tinggi membuat saldo stok bertambah ganda jika pembaruan saldo tidak dilindungi deduplikasi persisten.

- **Pola Integrasi**: **Work Queue** dengan satu exchange `inventory` bertipe **Direct**, routing key `stock.received`, antrean tunggal `stock_updates`, dan model `competing consumer`.
- **Hasil Minimum yang Wajib Dibuktikan**:
  1. **Idempotensi**: Satu mutasi stok hanya tercatat 1 kali per `event_id` tanpa penambahan saldo ganda saat terjadi event replay.
  2. **Konsistensi Ledger**: Rangkaian pengujian bersama U1–U4 menghasilkan tepat **26 record ledger valid**.
  3. **Formula Saldo Stok**:  
     $$\text{Saldo Akhir} = \text{Saldo Awal (100)} + \text{Total Quantity 26 Event Valid} = 100 + 90 = 190$$
  4. **Dead Letter Handling**: Pesan tidak valid (`X01`, tanpa field SKU) otomatis dialihkan ke antrean DLQ dan dicatat ke tabel `rejected_events` tanpa mengubah saldo bisnis barang.

---

## 3. Prasyarat Sistem & Dependensi

Pastikan perangkat kerja telah memenuhi spesifikasi berikut:

- **Node.js**: Versi 20.6 ke atas (disarankan Node.js 20 LTS atau Node.js 22/24) dengan dukungan bawaan `--env-file`.
- **Docker & Docker Compose**: Untuk menjalankan container RabbitMQ dan PostgreSQL secara terisolasi.
- **Port yang Dibutuhkan**:
  - `5672` (RabbitMQ AMQP)
  - `15672` (RabbitMQ Management Dashboard)
  - `5432` (PostgreSQL Database)
  - `3010` (HTTP Producer API Gateway - Opsional)

---

## 4. Konfigurasi Environment (`.env`)

Salin template konfigurasi dari `.env.contoh`:

```bash
cp .env.contoh .env
```

Isi konfigurasi default pada file `.env`:

```ini
# Konfigurasi RabbitMQ Broker
RABBITMQ_USER=simpel
RABBITMQ_PASS=simpel123
RABBITMQ_VHOST=/
RABBITMQ_PORT=5672
RABBITMQ_UI_PORT=15672
AMQP_URL=amqp://simpel:simpel123@localhost:5672

# Konfigurasi PostgreSQL
POSTGRES_USER=simpel
POSTGRES_PASSWORD=simpel123
POSTGRES_DB=simpel
POSTGRES_PORT=5432
DATABASE_URL=postgres://simpel:simpel123@localhost:5432/simpel

# Konfigurasi Service & Worker
PORT_PRODUCER=3010
WORKER_PREFETCH=1
WORKER_KERJA_MS=50
WORKER_ID=worker-stok-1
```

> **Akses Dashboard RabbitMQ Management**:
>
> - **URL**: [http://localhost:15672](http://localhost:15672)
> - **Username**: `simpel`
> - **Password**: `simpel123`

---

## 5. Arsitektur & Topologi Broker

```mermaid
flowchart LR
    subgraph Producer Layer
        P1["Producer HTTP API<br/>(Express 3010)"]
        P2["Producer CLI / Test Automator"]
    end

    subgraph Message Broker["Message Broker (RabbitMQ)"]
        EX["Exchange: inventory<br/>(Direct, Durable)"]
        Q["Queue: stock_updates<br/>(Durable, Ack: Manual)"]
        DLX["DLX: inventory.dlx<br/>(Direct, Durable)"]
        DLQ["DLQ: stock_updates.dlq<br/>(Durable)"]
    end

    subgraph Consumer and Storage Layer
        W["Worker Consumer<br/>(Competing Instance)"]

        subgraph Database
            DB[("PostgreSQL 16<br/>Database")]
            TBL1["stok_barang<br/>(Saldo Real-time)"]
            TBL2["ledger_penerimaan<br/>(Audit Trail & Idempotensi)"]
            TBL3["rejected_events<br/>(Dead Letter Log)"]
        end
    end

    %% Producer to Exchange
    P1 -->|"stock.received"| EX
    P2 -->|"stock.received"| EX

    %% Exchange to Queue
    EX -->|"Routing Key: stock.received"| Q

    %% Queue to Worker
    Q -->|"channel.consume (prefetch=5)"| W

    %% Worker Processing Flow
    W -->|"1. BEGIN Transaction"| DB
    W -->|"2. Check event_id (FOR UPDATE)"| TBL2
    W -->|"3. Update Saldo (FOR UPDATE)"| TBL1
    W -->|"4. Insert Ledger Mutasi"| TBL2
    W -->|"5. COMMIT & channel.ack"| DB

    %% Rejection Flow
    W -->|"Rejected/Cacat (nack no-requeue)"| DLX
    DLX -->|"Routing Key: stock.rejected"| DLQ
    W -->|"Insert Log Penolakan"| TBL3

    %% Styling
    style Producer Layer fill:#f5f5f5,stroke:#333
    style Message Broker fill:#e8f4f8,stroke:#333
    style Consumer and Storage Layer fill:#f0fff0,stroke:#333
    style Database fill:#fafafa,stroke:#999
    style EX stroke:#2196F3,stroke-width:2px
    style Q stroke:#2196F3,stroke-width:2px
    style W stroke:#4CAF50,stroke-width:2px
    style DB stroke:#ff9800,stroke-width:2px
```

---

## 6. Spesifikasi Kontrak Event

Format event JSON yang dipublikasikan ke broker:

```json
{
  "event_id": "run01-N01",
  "event_type": "stock.received",
  "occurred_at": "2026-09-23T08:00:00.000Z",
  "payload": {
    "receipt_id": "RCV-001",
    "sku": "SKU-001",
    "quantity": 3
  }
}
```

- **Aturan Validasi**:
  - `event_id`: String unik tidak boleh kosong (kunci idempotensi).
  - `event_type`: Wajib bernilai `stock.received`.
  - `occurred_at`: String timestamp berformat ISO-8601 valid.
  - `payload.receipt_id`: Nomor tanda terima fisik (string non-empty).
  - `payload.sku`: Kode item produk (string non-empty).
  - `payload.quantity`: Bilangan bulat positif $> 0$.

---

## 7. Urutan Perintah Operasional

Berikut adalah panduan lengkap dari penyiapan (setup), menjalankan layanan (start), pengiriman pesan (publish), pengujian (test), hingga penghentian sistem (stop):

### Langkah 1: Setup Lingkungan & Database

```bash
# 1. Jalankan container RabbitMQ & PostgreSQL
docker compose up -d

# 2. Inisialisasi skema tabel database (DDL)
npm run db:siapkan

# 3. Reset saldo awal SKU-001 = 100 dan kosongkan riwayat ledger
npm run db:reset
```

### Langkah 2: Start Layanan Consumer Worker

Jalankan consumer worker pada **Terminal 1**:

```bash
npm run worker
```

_(Worker akan terhubung ke RabbitMQ, mendengarkan antrean `stock_updates`, dan memproses event secara transaksional ke PostgreSQL)._

### Langkah 3: Start Layanan Producer API (Opsional)

Jika ingin menggunakan antarmuka HTTP REST API, buka **Terminal 2**:

```bash
npm run producer:api
```

_(Layanan HTTP aktif di `http://127.0.0.1:3010/penerimaan`)_.

### Langkah 4: Publish Event Manual / Ad-Hoc

Pengiriman pesan event dapat dilakukan melalui CLI maupun HTTP:

```bash
# Opsi 4A: Mengirim 1 event via CLI Producer
npm run producer:cli -- --eventId=EVT-MANUAL-01 --receiptId=RCV-001 --sku=SKU-001 --qty=5

# Opsi 4B: Mengirim kumpulan event sintetis (misal 5 event qty=3)
npm run kirim -- --count=5 --sku=SKU-001 --qty=3 --prefix=M

# Opsi 4C: Mengirim via HTTP REST API
curl -X POST http://127.0.0.1:3010/penerimaan \
  -H "Content-Type: application/json" \
  -d '{"event_id":"HTTP-001","receipt_id":"RCV-H01","sku":"SKU-001","quantity":5}'
```

### Langkah 5: Test & Uji Skenario

Eksekusi pengujian otomatis pada **Terminal Terpisah**:

```bash
# Opsi 5A: Menjalankan Seluruh Skenario Terpadu (U1 s.d U4) Sekaligus
npm run uji

# Opsi 5B: Menjalankan Model Pengujian Terpisah Per Skenario
npm run uji:u1   # Skenario U1: Beban Awal Normal 20 event (N01-N20, qty=3) -> Target Saldo = 160
npm run uji:u2   # Skenario U2: Pemulihan Worker Downtime & 5 event (G01-G05, qty=4) -> Target Saldo = 180
npm run uji:u3   # Skenario U3: Idempotensi Replay 5 event lama (N01-N05) -> Saldo & Ledger tetap
npm run uji:u4   # Skenario U4: Dead Letter X01 (cacat) & V01 (valid, qty=10) -> Target Saldo = 190, Ledger = 26
```

### Langkah 6: Stop & Pembersihan Layanan

```bash
# Menghentikan worker / producer yang aktif di terminal:
# Tekan Ctrl + C pada masing-masing jendela terminal

# Menghentikan container Docker:
docker compose stop

# Atau menghentikan dan menghapus container beserta jaringan:
docker compose down
```

---

## 8. Cara Memeriksa Hasil & Bukti Pengujian

Hasil pengolahan data dan status pengujian dapat diverifikasi melalui beberapa metode:

### 1. Memeriksa Rekapitulasi Ringkas via CLI Tool

Jalankan perintah berikut kapan saja:

```bash
npm run hasil
```

Output JSON yang ditampilkan:

```json
{
  "runId": "ALL",
  "saldo_sku001": 190,
  "total_ledger_rows": 26,
  "total_quantity_masuk": 90,
  "total_rejected_events": 1
}
```

### 2. Memeriksa File Bukti Uji (.evidence)

Setiap pengujian otomatis menghasilkan artefak JSON di dalam folder `.evidence/`:

- `.evidence/<runId>-u1.json`: Bukti status lulus skenario beban awal normal.
- `.evidence/<runId>-u2.json`: Bukti penerimaan pesan pasca pemulihan worker.
- `.evidence/<runId>-u3.json`: Bukti pencegahan mutasi ganda saat replay.
- `.evidence/<runId>-u4.json`: Bukti penolakan pesan cacat `X01` dan pemrosesan `V01`.

### 3. Memeriksa Langsung ke Basis Data PostgreSQL

Gunakan `psql` atau database client:

```bash
# Masuk ke CLI container PostgreSQL
docker compose exec postgres psql -U simpel -d simpel

-- Periksa saldo stok saat ini:
SELECT * FROM stok_barang WHERE sku = 'SKU-001';

-- Periksa 5 mutasi ledger terakhir:
SELECT * FROM ledger_penerimaan ORDER BY event_id DESC LIMIT 5;

-- Periksa log pesan cacat di tabel dead letter:
SELECT event_id, alasan, terjadi_pada FROM rejected_events;
```

### 4. Memeriksa Antrean & DLQ pada RabbitMQ Web UI

1. Akses browser ke [http://localhost:15672](http://localhost:15672) (User: `simpel` / Pass: `simpel123`).
2. Masuk ke tab **Queues**:
   - `stock_updates`: Harus bernilai `Ready: 0` (menunjukkan semua pesan habis dikonsumsi oleh worker).
   - `stock_updates.dlq`: Berisi pesan reject yang masuk melalui exchange DLX.
3. Masuk ke tab **Exchanges** untuk melihat throughput `inventory` dan `inventory.dlx`.

---

## 9. Matriks Kriteria Keberhasilan Skenario (Halaman 7)

| Skenario | Deskripsi Uji                                                              |  Target Ledger   |   Target Saldo    | Bukti Keberhasilan                                                               |  Status   |
| :------- | :------------------------------------------------------------------------- | :--------------: | :---------------: | :------------------------------------------------------------------------------- | :-------: |
| **U1**   | Kirim 20 event valid (`N01`–`N20`, qty: 3 per event)                       |      **20**      |      **160**      | 20 record tercatat di ledger, saldo awal 100 naik menjadi 160.                   | **LULUS** |
| **U2**   | Simulasi worker down, kirim 5 event (`G01`–`G05`, qty: 4), hidupkan worker |      **25**      |      **180**      | Pesan tertahan di broker saat down, diproses tuntas saat worker pulih.           | **LULUS** |
| **U3**   | Replay 5 event lama (`N01`–`N05`) dengan payload persis sama               | **25** _(tetap)_ | **180** _(tetap)_ | Idempotensi aktif: duplikasi terdeteksi, saldo dan ledger tidak bertambah ganda. | **LULUS** |
| **U4**   | Kirim 1 event cacat `X01` (tanpa `sku`) & 1 event valid `V01` (qty: 10)    |      **26**      |      **190**      | `X01` dialihkan ke DLQ/`rejected_events`, `V01` berhasil diproses ke saldo 190.  | **LULUS** |
