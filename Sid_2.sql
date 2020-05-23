-- MySQL dump 10.13  Distrib 8.0.18, for Win64 (x86_64)
--
-- Host: localhost    Database: sid_2
-- ------------------------------------------------------
-- Server version	8.0.18

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `alerta`
--

DROP TABLE IF EXISTS `alerta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `alerta` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `DataHoraMedicao` timestamp NOT NULL,
  `TipoSensor` varchar(3) NOT NULL,
  `ValorMedicao` decimal(6,2) NOT NULL,
  `Limite` decimal(6,2) NOT NULL,
  `Descricao` varchar(1000) NOT NULL,
  `Controlo` tinyint(4) DEFAULT NULL,
  `Extra` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `ID_UNIQUE` (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `alerta`
--

LOCK TABLES `alerta` WRITE;
/*!40000 ALTER TABLE `alerta` DISABLE KEYS */;
/*!40000 ALTER TABLE `alerta` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `diasemana`
--

DROP TABLE IF EXISTS `diasemana`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `diasemana` (
  `DiaSemana` varchar(20) NOT NULL,
  `HoraRonda` varchar(20) NOT NULL,
  PRIMARY KEY (`DiaSemana`,`HoraRonda`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `diasemana`
--

LOCK TABLES `diasemana` WRITE;
/*!40000 ALTER TABLE `diasemana` DISABLE KEYS */;
/*!40000 ALTER TABLE `diasemana` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `medicaosensores`
--

DROP TABLE IF EXISTS `medicaosensores`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `medicaosensores` (
  `ValorMedicao` decimal(6,2) NOT NULL,
  `TipoSensor` varchar(3) NOT NULL,
  `DataHoraMedicao` timestamp NOT NULL,
  `Controlo` tinyint(4) DEFAULT NULL,
  `Extra` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`ValorMedicao`,`TipoSensor`,`DataHoraMedicao`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `medicaosensores`
--

LOCK TABLES `medicaosensores` WRITE;
/*!40000 ALTER TABLE `medicaosensores` DISABLE KEYS */;
/*!40000 ALTER TABLE `medicaosensores` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ronda_extra`
--

DROP TABLE IF EXISTS `ronda_extra`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ronda_extra` (
  `email` varchar(100) NOT NULL,
  `ronda_inicio` timestamp NOT NULL,
  `ronda_fim` timestamp NOT NULL,
  PRIMARY KEY (`email`,`ronda_inicio`),
  CONSTRAINT `fk_ronda_inicio_email` FOREIGN KEY (`email`) REFERENCES `utilizador` (`EmailUtilizador`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ronda_extra`
--

LOCK TABLES `ronda_extra` WRITE;
/*!40000 ALTER TABLE `ronda_extra` DISABLE KEYS */;
/*!40000 ALTER TABLE `ronda_extra` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ronda_planeada`
--

DROP TABLE IF EXISTS `ronda_planeada`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ronda_planeada` (
  `email` varchar(100) NOT NULL,
  `ronda_inicio` timestamp NOT NULL,
  `ronda_fim` timestamp NOT NULL,
  PRIMARY KEY (`email`,`ronda_inicio`),
  CONSTRAINT `fk_ronda_fim_email` FOREIGN KEY (`email`) REFERENCES `utilizador` (`EmailUtilizador`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ronda_planeada`
--

LOCK TABLES `ronda_planeada` WRITE;
/*!40000 ALTER TABLE `ronda_planeada` DISABLE KEYS */;
/*!40000 ALTER TABLE `ronda_planeada` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sistema`
--

DROP TABLE IF EXISTS `sistema`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sistema` (
  `LimiteTemperatura` decimal(6,2) NOT NULL,
  `LimiteHumidade` decimal(6,2) NOT NULL,
  `LimiteLuminosidade` decimal(6,2) NOT NULL,
  `margem_temperatura` decimal(6,2) NOT NULL,
  `margem_humidade` decimal(6,2) NOT NULL,
  `margem_luminosidade` decimal(6,2) NOT NULL,
  `num_mov_alerta` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sistema`
--

LOCK TABLES `sistema` WRITE;
/*!40000 ALTER TABLE `sistema` DISABLE KEYS */;
INSERT INTO `sistema` VALUES (50.00,30.00,20.00,0.00,0.00,0.00,0);
/*!40000 ALTER TABLE `sistema` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `utilizador`
--

DROP TABLE IF EXISTS `utilizador`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `utilizador` (
  `EmailUtilizador` varchar(100) NOT NULL,
  `NomeUtilizador` varchar(200) NOT NULL,
  `TipoUtilizador` varchar(3) DEFAULT NULL,
  PRIMARY KEY (`EmailUtilizador`),
  UNIQUE KEY `EmailUtilizador_UNIQUE` (`EmailUtilizador`),
  UNIQUE KEY `NomeUtilizador_UNIQUE` (`NomeUtilizador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `utilizador`
--

LOCK TABLES `utilizador` WRITE;
/*!40000 ALTER TABLE `utilizador` DISABLE KEYS */;
/*!40000 ALTER TABLE `utilizador` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-05-23 22:25:06
