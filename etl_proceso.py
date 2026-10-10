import pandas as pd
import mysql.connector
from mysql.connector import Error

class ETLProcesador:
    def __init__(self, archivo_csv, db_config):
        self.archivo_csv = archivo_csv
        self.db_config = db_config
        self.data = None

    def extraer(self):
        """Fase 1: Extracción del archivo plano CSV con manejo de excepciones"""
        print(">>> Iniciando Fase de Extracción...")
        try:
            # Leer el archivo CSV
            self.data = pd.read_csv(self.archivo_csv)
            print(f"Extracción exitosa. Registros leídos inicialmente: {len(self.data)}")
            return True
        except FileNotFoundError:
            print(f"[Error]: El archivo '{self.archivo_csv}' no fue encontrado en la ruta especificada.")
            return False
        except Exception as e:
            print(f"[Error en Lectura]: {e}")
            return False

    def transformar(self):
        """Fase 2: Transformación, Limpieza y Normalización de datos"""
        if self.data is None:
            print("[Error]: No hay datos extraídos para transformar.")
            return False
        
        print("\n>>> Iniciando Fase de Transformación...")
        try:
            df = self.data.copy()

            # 1. Limpieza de nulos (NaN) en campos esenciales
            df = df.dropna(subset=['id_transaccion', 'cantidad', 'precio_unitario'])

            # 2. Eliminar espacios en blanco sobrantes en campos de texto (.strip())
            columnas_texto = df.select_dtypes(include=['object']).columns
            for col in columnas_texto:
                df[col] = df[col].astype(str).str.strip()

            # 3. Descartar filas donde cantidad o precio_unitario sea menor o igual a cero
            df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce')
            df['precio_unitario'] = pd.to_numeric(df['precio_unitario'], errors='coerce')
            df = df[(df['cantidad'] > 0) & (df['precio_unitario'] > 0)]

            # 4. Normalización (Productos a MAYÚSCULAS y correos a minúsculas)
            if 'producto' in df.columns:
                df['producto'] = df['producto'].str.upper()
            if 'correo_empleado' in df.columns:
                df['correo_empleado'] = df['correo_empleado'].str.lower()

            # 5. Cálculo derivado: total_venta = cantidad * precio_unitario
            df['total_venta'] = df['cantidad'] * df['precio_unitario']

            # 6. Estandarizar formato de fechas a formato MySQL (YYYY-MM-DD)
            df['fecha_registro'] = pd.to_datetime(df['fecha_registro'], errors='coerce')
            df = df.dropna(subset=['fecha_registro'])  # Quitar filas si la fecha quedó corrupta o inválida
            df['fecha_registro'] = df['fecha_registro'].dt.strftime('%Y-%m-%d')

            self.data = df
            print(f"Transformación exitosa. Registros limpios listos para cargar: {len(self.data)}")
            return True
        except Exception as e:
            print(f"[Error en Transformación]: {e}")
            return False

    def cargar(self):
        """Fase 3: Conexión y carga masiva parametrizada en MySQL con control transaccional"""
        if self.data is None or self.data.empty:
            print("[Error]: No hay datos válidos para cargar.")
            return False

        print("\n>>> Iniciando Fase de Carga...")
        conexion = None
        cursor = None
        try:
            # Establecer conexión con MySQL
            conexion = mysql.connector.connect(**self.db_config)
            cursor = conexion.cursor()

            # Crear la tabla automáticamente si no existe previamente
            query_tabla = """
            CREATE TABLE IF NOT EXISTS historial_ventas (
                id_transaccion VARCHAR(50) PRIMARY KEY,
                producto VARCHAR(100),
                cantidad INT,
                precio_unitario DECIMAL(10, 2),
                fecha_registro DATE,
                correo_empleado VARCHAR(100),
                total_venta DECIMAL(12, 2)
            );
            """
            cursor.execute(query_tabla)
            
            # Preparar la inserción masiva y manejo parametrizado
            query_insertar = """
            INSERT INTO historial_ventas (id_transaccion, producto, cantidad, precio_unitario, fecha_registro, correo_empleado, total_venta)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                producto=VALUES(producto), cantidad=VALUES(cantidad), 
                precio_unitario=VALUES(precio_unitario), fecha_registro=VALUES(fecha_registro), 
                correo_empleado=VALUES(correo_empleado), total_venta=VALUES(total_venta);
            """
            
            # Formatear el DataFrame limpio a una lista de tuplas nativas de Python
            registros = list(self.data[[
                'id_transaccion', 'producto', 'cantidad', 'precio_unitario', 
                'fecha_registro', 'correo_empleado', 'total_venta'
            ]].itertuples(index=False, name=None))

            # Ejecución en lote (masiva)
            cursor.executemany(query_insertar, registros)
            
            # Asegurar uso de transacciones: Confirmar cambios
            conexion.commit()
            print(f"Carga finalizada con éxito. Se procesaron {cursor.rowcount} operaciones en la tabla destino.")
            return True

        except Error as e:
            # Manejo de rollback ante errores imprevistos de base de datos
            if conexion:
                conexion.rollback()
                print("[Rollback]: Ocurrió un fallo en la base de datos. Se deshicieron todos los cambios pendientes.")
            print(f"[Error de MySQL]: {e}")
            return False
            
        finally:
            # Garantizar cierre de punteros y sesión
            if cursor:
                cursor.close()
            if conexion and conexion.is_connected():
                conexion.close()

# --- Bloque de Orquestación Principal ---
if __name__ == "__main__":
    # Configuración parametrizada con tus credenciales asignadas
    config_base_datos = {
        'host': 'localhost',
        'user': 'root',
        'password': 'jcay2412',
        'database': 'almacen_db'
    }

    # Instanciar el flujo ETL asociando el dataset local
    proceso = ETLProcesador(archivo_csv='transacciones_almacen.csv', db_config=config_base_datos)

    # Lanzamiento secuencial del pipeline
    if proceso.extraer():
        if proceso.transformar():
            proceso.cargar()
