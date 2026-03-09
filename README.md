# Proyecto preparación de datos

## Modelo Relacional – E-commerce

~~~ mermaid
erDiagram

    CLIENTES ||--o{ VENTAS : realiza
    PRODUCTOS ||--o{ VENTAS : contiene
    CATEGORIAS ||--o{ PRODUCTOS : clasifica

    CLIENTES {
        int cliente_id PK
        string nombre
        string apellido
        string email
        string genero
        date fecha_registro
        string region
        int ingreso_mensual
        boolean activo
    }

    VENTAS {
        string venta_id PK
        int cliente_id FK
        date fecha_venta
        int producto_id FK
        int cantidad
        int precio_unitario
        int total_venta
        string canal_venta
    }

    PRODUCTOS {
        int producto_id PK
        string nombre_producto
        int categoria_id FK
    }

    CATEGORIAS {
        int categoria_id PK
        string nombre_categoria
    }
~~~