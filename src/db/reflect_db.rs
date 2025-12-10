use tokio_postgres::Client;
use std::collections::HashMap;
use crate::db::schema_structs::{Schema, Table, Column, ForeignKey, Index};
use toml;
use std::fs;
use anyhow::Result;

pub async fn reflect_db(client: &Client) -> Result<()> {
    // --- Get all tables in public schema ---
    let table_rows = client.query(
        "
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE';
        ",
        &[],
    ).await?;

    let mut tables = HashMap::new();

    for row in table_rows {
        let table_name: String = row.get("table_name");

        // --- Primary keys ---
        let pk_query = format!("
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a
              ON a.attrelid = i.indrelid
             AND a.attnum = ANY(i.indkey)
            JOIN pg_class c
              ON c.oid = i.indrelid
            WHERE c.relname = '{}'
              AND i.indisprimary;
        ", table_name);

        let pk_rows = client.query(&pk_query, &[]).await?;
        let pk_columns: Vec<String> = pk_rows.iter().map(|r| r.get("attname")).collect();

        // --- Columns ---
        let col_rows = client.query(
            "
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = $1;
            ",
            &[&table_name],
        ).await?;

        let mut columns = Vec::new();
        for col in col_rows {
            let name: String = col.get("column_name");
            let data_type: String = col.get("data_type");
            let nullable: String = col.get("is_nullable");
            let default: Option<String> = col.get("column_default");

            columns.push(Column {
                name: name.clone(),
                r#type: data_type,
                nullable: nullable == "YES",
                default,
                references: None, // populated below
                check: None,      // optional: can add check constraints
            });
        }

        // --- Foreign keys ---
        let fk_rows = client.query(
            "
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column,
                rc.update_rule AS on_update,
                rc.delete_rule AS on_delete
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.referential_constraints AS rc
              ON rc.constraint_name = tc.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_name = $1;
            ",
            &[&table_name],
        ).await?;

        for fk in fk_rows {
            let col_name: String = fk.get("column_name");
            let foreign_table: String = fk.get("foreign_table");
            let foreign_column: String = fk.get("foreign_column");
            let on_delete: Option<String> = fk.get("on_delete");
            let on_update: Option<String> = fk.get("on_update");

            if let Some(col) = columns.iter_mut().find(|c| c.name == col_name) {
                col.references = Some(ForeignKey {
                    table: foreign_table,
                    column: foreign_column,
                    on_delete,
                    on_update,
                });
            }
        }

        // --- Indexes (non-primary) ---
        let idx_query = format!("
            SELECT
                i.relname AS index_name,
                array_to_string(array_agg(a.attname), ',') AS columns,
                ix.indisunique AS is_unique
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relname = '{}'
              AND NOT ix.indisprimary
            GROUP BY i.relname, ix.indisunique;
        ", table_name);

        let idx_rows = client.query(&idx_query, &[]).await?;
        let mut indexes = Vec::new();

        for idx in idx_rows {
            let idx_name: String = idx.get("index_name");
            let idx_columns: String = idx.get("columns");
            let is_unique: bool = idx.get("is_unique");

            let cols_vec: Vec<String> = idx_columns
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();

            indexes.push(Index {
                name: idx_name,
                columns: cols_vec,
                unique: is_unique,
            });
        }

        tables.insert(table_name.clone(), Table {
            pk: pk_columns,
            columns,
            indexes,
        });
    }

    // --- Build schema ---
    let schema = Schema {
        version: 1,
        tables,
    };

    // --- Write TOML ---
    fs::create_dir_all("src/schema")?;
    fs::write("src/schema/schema.toml", toml::to_string_pretty(&schema)?)?;
    println!("Generated schema/src/schema.toml from database!");

    Ok(())
}
