extern crate rusqlite;
use rusqlite::Connection;
use std::path::{Path};
use std::fmt;

#[macro_use]
extern crate serde_json;

use self::serde_json::{Value};

#[derive(Debug)]
struct Node {
  id: i64,
  name: String,
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "node({}): {}", self.id, self.name)
    }
}

impl Node {
  fn new(conn: &Connection, name: &str) -> Result<Node, rusqlite::Error> {
      let result = conn.execute("
                   INSERT INTO node (name)
                   VALUES (?1)",
                   &[&name.to_string()]);
      match result {
          Ok(_) => {
              Ok(Node {
                  id: conn.last_insert_rowid(),
                  name: name.to_string(),
              })
          },
          Err(issue) => {
              println!("Error inserting Node {} {:?}", name.to_string(), issue);
              Err(issue)
          },
      }
    
  }

  fn get(conn: &Connection, id: i64) -> Option<Node> {
    let mut statement = conn.prepare("
                        SELECT id, name
                        FROM node
                        WHERE id = :id").unwrap();
    let mut rows = statement.query_named(&[(":id", &id)]).unwrap();
    let rowResult = rows.next().unwrap(); // UGLY! fix me!
    match rowResult {
        Ok(rowResult) => Some(Node{
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
   predicate_id: i64,
   object_id: i64,
}


impl fmt::Display for Triple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "triple({}): ({} -[{}]-> {})",
               self.id, self.subject_id, self.predicate_id, self.object_id)
    }
}

impl Triple {
    fn new(conn: &Connection, subject: &Node, predicate: &Node, object: &Node) -> Result<Triple, rusqlite::Error> {
        let result = conn.execute("
                     INSERT INTO triple (subject_id, predicate_id, object_id)
                     VALUES (?1, ?2, ?3)",
                     &[&subject.id, &predicate.id, &object.id]);
        match result {
            Ok(_) => Ok(Triple {
                id: conn.last_insert_rowid(),
                subject_id: subject.id,
                predicate_id: predicate.id,
                object_id: object.id 
            }),
            Err(issue) => { 
                println!("Error inserting Triple {:?}", issue);
                Err(issue)
            }
        }
    }

    fn get(conn: &Connection, id: i64) -> Option<Triple> {
        let mut statement = conn.prepare("
                        SELECT id,
                               subject_id,
                               predicate_id,
                               object_id
                        FROM triple
                        WHERE id = :id").unwrap();
        let mut rows = statement.query_named(&[(":id", &id)]).unwrap();
        let rowResult = rows.next().unwrap();
        match rowResult {
            Ok(rowResult) => Some(Triple{
                id: rowResult.get(0),
                subject_id: rowResult.get(1),
                predicate_id: rowResult.get(2),
                object_id: rowResult.get(3),
            }),
            Err(_) => None,
        }
    }
}

fn build_database(location: Option<&'static str>) -> Result<Connection, rusqlite::Error> {
    let mut conn = match location {
        Some(location) => Connection::open(Path::new(location)).unwrap(),
        None => Connection::open_in_memory().unwrap(),
    };
    
   let result = conn.execute("CREATE TABLE IF NOT EXISTS node (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       name VARCHAR,
       properties JSON,
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
        subject_id INTEGER NOT NULL,
        predicate_id INTEGER NOT NULL,
        object_id INTEGER NOT NULL,
        FOREIGN KEY(subject_id) REFERENCES node(id),
        FOREIGN KEY(predicate_id) REFERENCES node(id),
        FOREIGN KEY(object_id) REFERENCES node(id),
        CONSTRAINT yadda UNIQUE (subject_id, predicate_id, object_id)
    )", &[]);
    match result {
       Ok(value) => println!("seems ok"),
       Err(value) => println!("{:?}nope", value),
    }

   Ok(conn)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn node_make() {
        assert_eq!(true, true);
        let mut conn = build_database(None).unwrap();
        let node1 = Node::new(&conn, "test node").unwrap();
        let node2 = Node::new(&conn, "test node the second").unwrap();
        assert_eq!(node1.id, 1);
        assert_eq!(node2.id, 2);
        let failed_state = match Node::new(&conn, "test node") {
            Ok(result) => false,
            Err(_) => true,
        };
        assert_eq!(failed_state, true);
        //let json = r#"{"foo": 13, "bar": "baz"}"#;
        //let data: serde_json::Value = serde_json::from_str(json).unwrap()
        let node_description = json!({
            "name": "some test name in json",
        });
    }
    
    #[test]
    fn triple_make() {
        let mut conn = build_database(None).unwrap();
        conn.execute("
        INSERT INTO node (name)
        VALUES 
        ('Boris the bullet dodger'),('Brick Top'),('Knows');
        ", &[]);
        let node1 = Node::get(&conn, 1).unwrap();
        let node2 = Node::get(&conn, 2).unwrap();
        let node3 = Node::get(&conn, 3).unwrap();
        assert_eq!("Boris the bullet dodger", &node1.name);     
        assert_eq!("Brick Top", &node2.name);
        let triple = Triple::new(&conn, &node1, &node3, &node2).unwrap();
        println!("{}", triple);
        let get_triple = Triple::get(&conn, 1).unwrap();
        assert_eq!(triple.id, get_triple.id);
        assert_eq!(triple.object_id, get_triple.object_id);
        assert_eq!(triple.subject_id, get_triple.subject_id);
        assert_eq!(triple.predicate_id, get_triple.predicate_id);
    }
}
