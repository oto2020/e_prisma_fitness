const { PrismaClient } = require('@prisma/client');
const xlsx = require('xlsx');
const fs = require('fs');
const prisma = new PrismaClient();

async function main() {
  // Период оказания услуг
  const startDateString = '2024-10-01';
  const endDateString = '2024-12-31';

  // Названия услуг
  const serviceName1 = '%тестирован%';
  const serviceName2 = '%в групповых%';
  const serviceName3 = '%в тренажерном%';
  const serviceName4 = '%в аква зоне%';

  const startDate = new Date(startDateString);
  const endDate = new Date(endDateString);

  const servicesSales = await prisma.$queryRaw`
    SELECT 
      services.name AS serviceName,
      services.datetime AS serviceDatetime,
      services.trainer AS serviceTrainer,
      IFNULL(TIMESTAMPDIFF(DAY, services.datetime, sales.datetime), 'N/A') AS saleAfterDays,
      sales.client AS saleClient,
      services.client AS serviceClient,
      sales.datetime AS saleDatetime,
      sales.division AS saleDivision,
      sales.name AS saleName,
      sales.trainer AS saleTrainer,
      sales.final_price AS saleFinalPrice
    FROM services
    LEFT JOIN sales ON sales.client = services.client AND TIMESTAMPDIFF(DAY, services.datetime, sales.datetime) >= 0
    WHERE (
        services.name LIKE ${serviceName1} OR 
        services.name LIKE ${serviceName2} OR 
        services.name LIKE ${serviceName3} OR 
        services.name LIKE ${serviceName4}
    )
      AND services.datetime BETWEEN ${startDate} AND ${endDate}
      AND (sales.final_price > 0 AND sales.final_price IS NOT NULL)
    ORDER BY services.datetime ASC, saleAfterDays ASC;
  `;

  // Преобразование данных в формат для xlsx
  const data = servicesSales.map(row => ({
    serviceName: row.serviceName,
    serviceDatetime: row.serviceDatetime,
    serviceTrainer: row.serviceTrainer,
    saleAfterDays: row.saleAfterDays,
    serviceClient: row.serviceClient,
    arrow: '->',
    // saleClient: row.saleClient,
    saleDatetime: row.saleDatetime,
    saleDivision: row.saleDivision,
    saleName: row.saleName,
    saleTrainer: row.saleTrainer,
    saleFinalPrice: row.saleFinalPrice,
  }));

  // Создание новой книги
  const workbook = xlsx.utils.book_new();
  const worksheet = xlsx.utils.json_to_sheet(data);

  // Добавление листа в книгу
  xlsx.utils.book_append_sheet(workbook, worksheet, 'ServicesSales');

  // Сохранение файла
  const outputPath = `services_sales_conversion ${startDateString} - ${endDateString}.xlsx`;
  xlsx.writeFile(workbook, outputPath);

  console.log(`Report saved to ${outputPath}`);
}

main()
  .catch(e => {
    console.error(e);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
