const { PrismaClient } = require('@prisma/client');
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');

const prisma = new PrismaClient();

async function loadServicesData(filePath) {
  const workbook = xlsx.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  let jsonData = xlsx.utils.sheet_to_json(worksheet);

  jsonData = jsonData.slice(1, -1); // Удаляем первую и последнюю строку

  const servicesData = jsonData.map(row => ({
    id: String(row.id),
    name: row.name,
    division: row.division,
    trainer: row.trainer,
    client: row.client,
    basis: row.basis || null,
    datetime: new Date(row.datetime),
    price: Number(row.price)
  }));

  console.log(`Подготовлено ${servicesData.length} записей.`);

  try {
    // Очистка таблицы перед загрузкой новых данных
    await prisma.service.deleteMany({});
    console.log("✅ Все старые данные удалены.");

    const batchSize = 500; // Вставка данных батчами по 500 записей
    for (let i = 0; i < servicesData.length; i += batchSize) {
      const batch = servicesData.slice(i, i + batchSize);
      console.log(`📤 Вставка записей: ${i + 1}-${i + batch.length} / ${servicesData.length}`);

      await prisma.service.createMany({
        data: batch,
        skipDuplicates: true
      });
    }

    console.log(`✅ Файл ${filePath} загружен.`);
  } catch (error) {
    console.error("❌ Ошибка при загрузке данных:", error);
  }
}

async function main() {
  const directoryPath = path.join(__dirname, "..", "ftp");

  fs.readdir(directoryPath, async (err, files) => {
    if (err) {
      return console.log('❌ Ошибка чтения директории: ' + err);
    }

    const excelFiles = files.filter(file => file.startsWith('ftp.services') && file.endsWith('.xlsx'));

    if (excelFiles.length === 0) {
      console.log("❌ Файлы для загрузки не найдены.");
      return;
    }

    console.log(`🔍 Найдено ${excelFiles.length} файлов для обработки.`);

    for (const file of excelFiles) {
      await loadServicesData(path.join(directoryPath, file));
    }

    console.log('🎉 Все файлы обработаны.');
  });
}

main()
  .catch(e => console.error(e))
  .finally(async () => {
    await prisma.$disconnect();
  });
