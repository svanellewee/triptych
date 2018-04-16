extern crate rusqlite;
use rusqlite::Connection;
use std::path::{Path};
use std::fmt;


#[derive(Debug)]
struct Entity {
  id: i64,
  name: String,
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "entity({}): {}", self.id, self.name)
    }
}

impl Entity {
  fn new(conn: &Connection, name: &str) -> Result<Entity, rusqlite::Error> {
      let result = conn.execute("
                   INSERT INTO entity (name)
                   VALUES (?1)",
                   &[&name.to_string()]);
      match result {
          Ok(_) => {
              Ok(Entity {
                  id: conn.last_insert_rowid(),
                  name: name.to_string(),
              })
          },
          Err(issue) => {
              println!("Error inserting Entity {} {:?}", name.to_string(), issue);
              Err(issue)
          },
      }
    
  }

  fn get(conn: &Connection, id: i64) -> Option<Entity> {
    let mut statement = conn.prepare("
                        SELECT id, name
                        FROM entity
                        WHERE id = :id").unwrap();
    let mut rows = statement.query_named(&[(":id", &id)]).unwrap();
    let rowResult = rows.next().unwrap(); // UGLY! fix me!
    match rowResult {
        Ok(rowResult) => Some(Entity{
            id: rowResult.get(0),
            name: rowResult.get(1),
        }),
        Err(_) => None,
    }
  }
}

#[derive(Debug)]
struct Triple {
   id: i64,
   subject_id: i64,
   predicate: String,
   object_id: i64,
}

impl fmt::Display for Triple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "triple({}): ({} -[{}]-> {})",
               self.id, self.subject_id, self.predicate, self.object_id)
    }
}

impl Triple {
    fn new(conn: &Connection, subject: &Entity, predicate: &str, object: &Entity) -> Result<Triple, rusqlite::Error> {
        let result = conn.execute("
                     INSERT INTO triple (subject_id, predicate, object_id)
                     VALUES (?1, ?2, ?3)",
                     &[&subject.id, &predicate, &object.id]);
        match result {
            Ok(_) => Ok(Triple {
                id: conn.last_insert_rowid(),
                subject_id: subject.id,
                predicate: predicate.to_string(),
                object_id: object.id 
            }),
            Err(issue) => { 
                println!("Error inserting Triple {:?}", issue);
                Err(issue)
            }
        }
    }
}

fn build_database(location: Option<&'static str>) -> Result<Connection, rusqlite::Error> {
    let mut conn = match location {
        Some(location) => Connection::open(Path::new(location)).unwrap(),
        None => Connection::open_in_memory().unwrap(),
    };
    
   let result = conn.execute("CREATE TABLE IF NOT EXISTS entity (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       name VARCHAR,
       CONSTRAINT unique_name UNIQUE(name)
    )", &[]);

    match conn.execute("PRAGMA foreign_keys = 1", &[]) {
        Err(value) => println!("Not right {:?}", value),
        _ => {},
    }
    match result {
       Ok(value) => println!("seems ok"),
       Err(value) => println!("{:?}nope", value),
    }
    
    let result = conn.execute("CREATE TABLE IF NOT EXISTS triple(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subject_id INTEGER,
        predicate VARCHAR,
        object_id INTEGER,
        FOREIGN KEY(subject_id) REFERENCES entity(id),
        FOREIGN KEY(object_id) REFERENCES entity(id),
        CONSTRAINT yadda UNIQUE (subject_id, predicate, object_id)
    )", &[]);
    match result {
       Ok(value) => println!("seems ok"),
       Err(value) => println!("{:?}nope", value),
    }

   Ok(conn)
}

/*fn main() {
    println!("Hello, world!");
    let mut conn = build_database("/tmp/yadda.db").unwrap();
    let stephan = Entity::new(&conn, "Stephan").unwrap();
    let joe = Entity::new(&conn, "Joe").unwrap();
    let color_red = Entity::new(&conn, "Red").unwrap();
    println!("{}..", stephan);
    println!("{}..", joe);
    println!("{}..", color_red);
    let stephan_knows_joe = Triple::new(&conn,
                                        &stephan,
                                        "knows",
                                        &joe).unwrap();
    println!("{}", stephan_knows_joe);
    let stephan_has_red_hair = Triple::new(&conn, &stephan, "haircolor", &color_red).unwrap();
    println!("{}", stephan_has_red_hair);
    conn.close();
}*/



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn entity_make() {
        assert_eq!(true, true);
        let mut conn = build_database(None).unwrap();
        let entity1 = Entity::new(&conn, "test entity").unwrap();
        let entity2 = Entity::new(&conn, "test entity the second").unwrap();
        assert_eq!(entity1.id, 1);
        assert_eq!(entity2.id, 2);
        let failed_state = match Entity::new(&conn, "test entity") {
            Ok(result) => false,
            Err(_) => true,
        };
        assert_eq!(failed_state, true);
    }
    
    #[test]
    fn triple_make() {
        let mut conn = build_database(None).unwrap();
        conn.execute("
        INSERT INTO entity (name)
        VALUES 
        ('Boris the bullet dodger'),('Brick Top');
        ", &[]);
        let entity1 = Entity::get(&conn, 1).unwrap();
        let entity2 = Entity::get(&conn, 2).unwrap();
        assert_eq!("Boris the bullet dodger", &entity1.name);     
        assert_eq!("Brick Top", &entity2.name);
        println!("{}", Triple::new(&conn, &entity1, "knows", &entity2).unwrap());
    }
}
