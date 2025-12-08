use tokio_postgres::{NoTls, Error};
use dotenv::dotenv;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv().ok(); // loads .env

    let host = env::var("DB_HOST").expect("DB_HOST not set");
    let user = env::var("DB_USER").expect("DB_USER not set");
    let password = env::var("DB_PASS").expect("DB_PASS not set");
    let dbname = env::var("DB_NAME").expect("DB_NAME not set");

    let conn_str = format!("host={} user={} password={} dbname={}", host, user, password, dbname);

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let rows = client.query("SELECT * FROM users;", &[]).await?;

    for row in rows {
        let id: i32 = row.get("id");
        let username: &str = row.get("username");
        let email: &str = row.get("email");

        println!("id={} username={} email={}", id, username, email);
    }

    Ok(())
}
