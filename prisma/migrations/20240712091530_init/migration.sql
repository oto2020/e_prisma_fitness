-- CreateTable
CREATE TABLE `sales` (
    `id` VARCHAR(555) NOT NULL,
    `datetime` DATETIME(3) NULL,
    `division` VARCHAR(191) NULL,
    `name` VARCHAR(191) NULL,
    `client` VARCHAR(191) NULL,
    `author` VARCHAR(191) NULL,
    `trainer` VARCHAR(191) NULL,
    `type` VARCHAR(191) NULL,
    `order_count` INTEGER NULL,
    `order_price` DOUBLE NULL,
    `refund_count` INTEGER NULL,
    `refund_price` DOUBLE NULL,
    `final_price` DOUBLE NULL,

    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `services` (
    `id` VARCHAR(555) NOT NULL,
    `name` VARCHAR(191) NULL,
    `division` VARCHAR(191) NULL,
    `trainer` VARCHAR(191) NULL,
    `client` VARCHAR(191) NULL,
    `basis` VARCHAR(191) NULL,
    `datetime` DATETIME(3) NULL,
    `price` DOUBLE NULL,

    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
