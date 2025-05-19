# Ingresos diarios por el servicio de Urgencias de los centros sanitarios en Canarias

**Descripción**: Ingresos diarios por el servicio de Urgencias de los centros sanitarios en Canarias durante la pandemia de COVID-19.  
**Autor**: Servicio Canario de la Salud

---

## Especificación de Campos

### 1. `fecha_datos`

- **Descripción**: Fecha en que se han extraído los datos de la base de datos de Sanidad.
- **Restricciones**:
  - Tipo: `http://www.w3.org/2001/XMLSchema#dateTime`
  - Formato de fecha: `%d/%m/%Y`

---

### 2. `codigo`

- **Descripción**: Código identificativo para cada centro.
- **Restricciones**:
  - Tipo: `http://www.w3.org/2001/XMLSchema#long`

---

### 3. `fecha`

- **Descripción**: Fecha de los datos.
- **Restricciones**:
  - Tipo: `http://www.w3.org/2001/XMLSchema#dateTime`
  - Formato de fecha: `%d/%m/%Y`

---

### 4. `serie`

- **Descripción**: Significado del campo `valor` (`Urg_cv`: casos que han ingresado en urgencias por COVID-19; `Urg_ingr`: ingresos totales hospitalarios).
- **Restricciones**:
  - Patrón: `(Urg_cv|Urg_ingr)`

---

### 5. `valor`

- **Descripción**: Número de ingresos del tipo definido en el campo `serie`.
- **Restricciones**:
  - Tipo: `http://www.w3.org/2001/XMLSchema#integer`

