from app.settings.settings import settings
from app.silver import loader, quality, transforms
from app.utils.logging import UnifiedLogger
from app.utils.manifest import bronze_unchanged
from app.utils.spark import SparkClient

_STEP_ORDER = ["quality", "schema", "load"]


class SilverPipeline:
    def __init__(self, spark_client: SparkClient):
        self.spark = spark_client.get_session()
        self.logger = UnifiedLogger("SilverPipeline", "silver")

    def _load_dims(self, stage):
        from app.silver import transforms

        self.logger.info("Cargando dimensiones desde disco/reconstruyendo...")
        return transforms.build_dims(self.spark, stage)

    def _load_stage(self):
        self.logger.info("Cargando datos de stage desde disco...")
        stage_dir = settings.project_root / settings.config.silver.path / "stage"
        result = {}

        # Load individual dataframes
        result["ingreso_unified"] = self.spark.read.parquet(
            str(stage_dir / "ingreso_unified.parquet")
        )
        result["rentas_preguntas"] = self.spark.read.parquet(
            str(stage_dir / "rentas_preguntas.parquet")
        )
        result["rentas_formulario"] = self.spark.read.parquet(
            str(stage_dir / "rentas_formulario.parquet")
        )
        result["rentas_esat"] = self.spark.read.parquet(
            str(stage_dir / "rentas_esat_estadistica_atm.parquet")
        )
        result["rentas_respuestas"] = self.spark.read.parquet(
            str(stage_dir / "rentas_respuestas.parquet")
        )
        result["rentas_ano_aplicacion"] = self.spark.read.parquet(
            str(stage_dir / "rentas_ano_aplicacion.parquet")
        )

        result["categorias_municipalidades"] = self.spark.read.parquet(
            str(stage_dir / "categorias_municipalidades.parquet")
        )

        # Load RENAMU DFs
        renamu_dfs = []
        for year in ("2021", "2022", "2023", "2024", "2025"):
            p = stage_dir / f"renamu_{year}.parquet"
            if p.exists():
                renamu_dfs.append(self.spark.read.parquet(str(p)))
        result["renamu_dfs"] = renamu_dfs

        # Load RENAMU 984 (legacy key for backward compat)
        p984 = stage_dir / "renamu_2025.parquet"
        if p984.exists():
            result["renamu_984"] = self.spark.read.parquet(str(p984))
        else:
            result["renamu_984"] = None

        self.logger.info("Datos de stage cargados exitosamente")
        return result

    def run(self, step: str | None = None, drop: bool = False, skip_unchanged: bool = False):
        self.logger.info("Iniciando Silver Pipeline")

        start_idx = _STEP_ORDER.index(step) if step else 0

        if skip_unchanged and start_idx == 0:
            bronze_manifest_path = settings.project_root / settings.config.api.path / "manifest.parquet"
            stage_manifest_path = settings.project_root / settings.config.silver.path / "stage" / "manifest.parquet"
            unchanged, detail = bronze_unchanged(bronze_manifest_path, stage_manifest_path)
            if unchanged:
                self.logger.info(
                    f"skip_unchanged: {detail} — datos sin cambios, omitiendo silver pipeline"
                )
                return
            else:
                self.logger.info(
                    f"skip_unchanged: {detail} — datos cambiados, ejecutando silver pipeline"
                )

        stage = None
        dims = None

        # Load stage/dims if skipped
        if start_idx > 0:
            stage = self._load_stage()
        if start_idx == 2:  # Step 'load'
            dims = self._load_dims(stage)

        for i, s in enumerate(_STEP_ORDER):
            if i < start_idx:
                continue

            if s == "quality":
                self.logger.info("Paso 1/3: Correcciones de calidad Bronze -> stage")
                try:
                    stage = quality.fix_all(self.spark)
                    self.logger.info("Paso 1/3 completado: Correcciones de calidad")
                except Exception as e:
                    self.logger.error(
                        f"Error en paso de calidad: {e}", stack_trace=True
                    )
                    raise
            elif s == "schema":
                self.logger.info(
                    "Paso 2/3: Construcción de dimensiones del modelo estrella"
                )
                try:
                    dims = transforms.build_dims(self.spark, stage)
                    self.logger.info("Paso 2/3 completado: Dimensiones construidas")
                except Exception as e:
                    self.logger.error(
                        f"Error en paso de esquema: {e}", stack_trace=True
                    )
                    raise
            elif s == "load":
                self.logger.info("Paso 3/3: Creación de tablas y carga en SQL Server")
                try:
                    loader.create_and_load(
                        spark=self.spark,
                        dims=dims,
                        stage=stage,
                        cfg=settings.config.silver,
                        drop=drop,
                    )
                    self.logger.info(
                        "Paso 3/3 completado: Datos cargados en SQL Server"
                    )
                except Exception as e:
                    self.logger.error(f"Error en paso de carga: {e}", stack_trace=True)
                    raise

        self.logger.info("Silver Pipeline completado exitosamente")
