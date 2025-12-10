use tokio_postgres::{NoTls};
use dotenv::dotenv;
use std::env;
use anyhow::Result;

mod db;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let host = env::var("DB_HOST").expect("DB_HOST not set");
    let user = env::var("DB_USER").expect("DB_USER not set");
    let password = env::var("DB_PASS").expect("DB_PASS not set");
    let dbname = env::var("DB_NAME").expect("DB_NAME not set");

    let conn_str = format!(
        "host={} user={} password={} dbname={}",
        host, user, password, dbname
    );

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    // Drive the connection
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    db::reflect_db::reflect_db(&client).await?;

    Ok(())
}
