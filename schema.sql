SET FOREIGN_KEY_CHECKS=0;

-- =====================================================================
-- 1. BORRADO DE TABLAS
-- =====================================================================
-- Se eliminan todas las tablas en el orden correcto para evitar problemas de dependencias.

DROP TABLE IF EXISTS `Hechos_Mantenimiento`;
DROP TABLE IF EXISTS `Predicciones_Vehiculo_Tipo`;
DROP TABLE IF EXISTS `Predicciones_Tipo_Mantenimiento`;
DROP TABLE IF EXISTS `Dim_Tiempo`;
DROP TABLE IF EXISTS `Dim_Terceros`;
DROP TABLE IF EXISTS `Dim_Vehiculos`;
DROP TABLE IF EXISTS `STG_Mantenimientos`;
DROP TABLE IF EXISTS `users`;

-- =====================================================================
-- 2. CREACIÓN DE TABLAS
-- =====================================================================

-- Tabla de usuarios para el login
CREATE TABLE `users` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `username` VARCHAR(50) UNIQUE NOT NULL,
    `password` VARCHAR(256) NOT NULL,
    `is_admin` BOOLEAN DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla de Staging para la carga inicial de datos
CREATE TABLE `STG_Mantenimientos` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `NombreVehiculo` VARCHAR(255),
    `TipoMatricula` VARCHAR(255),
    `Categoria` VARCHAR(255),
    `IdentificacionTercero` VARCHAR(255),
    `NombreTercero` VARCHAR(255),
    `TipoMantenimiento` VARCHAR(255),
    `Debito` DECIMAL(18, 2),
    `FechaElaboracion` DATETIME,
    `FechaCarga` DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Dimension: Vehículos
CREATE TABLE `Dim_Vehiculos` (
    `VehiculoKey` INT AUTO_INCREMENT PRIMARY KEY,
    `NombreVehiculo` VARCHAR(255) UNIQUE,
    `TipoMatricula` VARCHAR(255),
    `Categoria` VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Dimension: Terceros
CREATE TABLE `Dim_Terceros` (
    `TerceroKey` INT AUTO_INCREMENT PRIMARY KEY,
    `IdentificacionTercero` VARCHAR(255) UNIQUE,
    `NombreTercero` VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Dimension: Tiempo
CREATE TABLE `Dim_Tiempo` (
    `TiempoKey` INT AUTO_INCREMENT PRIMARY KEY,
    `Fecha` DATE UNIQUE,
    `Anio` INT,
    `Mes` INT,
    `Dia` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla de Hechos: Mantenimientos
CREATE TABLE `Hechos_Mantenimiento` (
    `HechoKey` INT AUTO_INCREMENT PRIMARY KEY,
    `VehiculoKey` INT,
    `TerceroKey` INT,
    `TiempoKey` INT,
    `TipoMantenimiento` VARCHAR(255),
    `Debito` DECIMAL(18, 2),
    FOREIGN KEY (`VehiculoKey`) REFERENCES `Dim_Vehiculos`(`VehiculoKey`),
    FOREIGN KEY (`TerceroKey`) REFERENCES `Dim_Terceros`(`TerceroKey`),
    FOREIGN KEY (`TiempoKey`) REFERENCES `Dim_Tiempo`(`TiempoKey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla para Predicciones por Vehículo y Tipo
CREATE TABLE `Predicciones_Vehiculo_Tipo` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `NombreVehiculo` VARCHAR(255) NOT NULL,
    `TipoMantenimiento` VARCHAR(255) NOT NULL,
    `Fecha` DATETIME NOT NULL,
    `Costo` DECIMAL(18, 2) NOT NULL,
    `Origen` VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla para Predicciones por Tipo de Mantenimiento General
CREATE TABLE `Predicciones_Tipo_Mantenimiento` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `TipoMantenimiento` VARCHAR(255) NOT NULL,
    `Fecha` DATETIME NOT NULL,
    `Costo` DECIMAL(18, 2) NOT NULL,
    `Origen` VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- =====================================================================
-- 3. PROCEDIMIENTOS ALMACENADOS
-- =====================================================================
-- Se eliminan los procedimientos si ya existen para evitar errores en la recreación.
DROP PROCEDURE IF EXISTS `sp_upsert_dimensiones`;
DROP PROCEDURE IF EXISTS `sp_insert_hechos`;

DELIMITER //

-- Procedimiento para poblar las dimensiones desde la tabla de staging
CREATE PROCEDURE `sp_upsert_dimensiones`()
BEGIN
    -- Upsert para Dim_Vehiculos
    INSERT INTO Dim_Vehiculos (NombreVehiculo, TipoMatricula, Categoria)
    SELECT DISTINCT NombreVehiculo, TipoMatricula, Categoria
    FROM STG_Mantenimientos stg
    WHERE stg.NombreVehiculo IS NOT NULL
    ON DUPLICATE KEY UPDATE
        TipoMatricula = VALUES(TipoMatricula),
        Categoria = VALUES(Categoria);

    -- Upsert para Dim_Terceros
    INSERT INTO Dim_Terceros (IdentificacionTercero, NombreTercero)
    SELECT DISTINCT IdentificacionTercero, NombreTercero
    FROM STG_Mantenimientos stg
    WHERE stg.IdentificacionTercero IS NOT NULL
    ON DUPLICATE KEY UPDATE
        NombreTercero = VALUES(NombreTercero);

    -- Upsert para Dim_Tiempo
    INSERT INTO Dim_Tiempo (Fecha, Anio, Mes, Dia)
    SELECT DISTINCT DATE(FechaElaboracion), YEAR(FechaElaboracion), MONTH(FechaElaboracion), DAY(FechaElaboracion)
    FROM STG_Mantenimientos stg
    WHERE stg.FechaElaboracion IS NOT NULL
    ON DUPLICATE KEY UPDATE
        Anio = VALUES(Anio),
        Mes = VALUES(Mes),
        Dia = VALUES(Dia);
END//

-- Procedimiento para poblar la tabla de hechos
CREATE PROCEDURE `sp_insert_hechos`()
BEGIN
    TRUNCATE TABLE Hechos_Mantenimiento;
    INSERT INTO Hechos_Mantenimiento (VehiculoKey, TerceroKey, TiempoKey, TipoMantenimiento, Debito)
    SELECT 
        v.VehiculoKey,
        t.TerceroKey,
        ti.TiempoKey,
        stg.TipoMantenimiento,
        stg.Debito
    FROM STG_Mantenimientos stg
    JOIN Dim_Vehiculos v ON stg.NombreVehiculo = v.NombreVehiculo
    JOIN Dim_Terceros t ON stg.IdentificacionTercero = t.IdentificacionTercero
    JOIN Dim_Tiempo ti ON DATE(stg.FechaElaboracion) = ti.Fecha;
END//

DELIMITER ;

-- Reactivar la verificación de claves foráneas
SET FOREIGN_KEY_CHECKS=1;

SELECT 'Estructura de la base de datos y procedimientos creados con éxito.' AS `Estado`;
