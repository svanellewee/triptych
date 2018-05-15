#[macro_use]
extern crate serde_json;

extern crate rusqlite;

use serde_json::{Value};
use rusqlite::{Connection};
use std::path::{Path};


#[derive(Debug)]
struct Node {
   id: Option<u32>,
   data: Value,
}

impl Node {
   pub fn create(db: &Connection, data: Value) -> Node {
     db.execute("
     CREATE TABLE IF NOT EXISTS node (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       data BLOB)", &[]).unwrap();

     db.execute("
     INSERT INTO node (data)
      VALUES (?)", &[&data]);
     
     Node {
         id: Some(db.last_insert_rowid() as u32),
         data: data
     }
   }
   pub fn get(db: &Connection, id: u32) -> Option<Node> {
      let mut stmnt = db.prepare("SELECT id, data
                                  FROM node
                                  WHERE id = :id")
          .unwrap();
      let mut rows = stmnt.query_named(&[(":id", &id)])
          .unwrap();
      let row_result = rows.next()
          .unwrap();
      match row_result {
          Ok(row_result) => Some(Node{
              id: Some(row_result.get(0)),
              data: row_result.get(1),
          }),
          Err(_) => None,
      }
   }
}

#[derive(Debug)]
struct Triple {
  id: Option<u32>,
  subject_id: u32,
  predicate_id: u32,
  object_id: u32,
}

impl Triple {
  pub fn create(db: &Connection,
                subject_id: u32,
                predicate_id: u32,
                object_id: u32) -> Triple {
     db.execute("
     CREATE TABLE IF NOT EXISTS triple (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       subject_id INTEGER NOT NULL,
       predicate_id INTEGER NOT NULL,
       object_id INTEGER NOT NULL,
       FOREIGN KEY(subject_id) REFERENCES node(id),
       FOREIGN KEY(predicate_id) REFERENCES node(id),
       FOREIGN KEY(object_id) REFERENCES node(id),
       CONSTRAINT only_unique_triples UNIQUE (subject_id, predicate_id, object_id))", &[]).unwrap();

     db.execute("
        INSERT INTO triple (subject_id, predicate_id, object_id)
        VALUES (?, ?, ?)", &[&subject_id, &predicate_id, &object_id]);
        Triple {
                id: Some(db.last_insert_rowid() as u32),
                subject_id: subject_id,
                predicate_id: predicate_id,
                object_id: object_id,
         }
     }  

  pub fn get(db: &Connection, id: u32) -> Option<Triple> {
      let mut stmnt = db.prepare("SELECT id, subject_id, predicate_id, object_id
                              FROM triple
                              WHERE id = :id").unwrap();
      let mut rows = stmnt.query_named(&[(":id", &id)]).unwrap();
      let row_result = rows.next().unwrap();
      match row_result {
          Ok(row_result) => Some(Triple{
              id: Some(row_result.get(0)),
              subject_id: row_result.get(1),
              predicate_id: row_result.get(2),
              object_id: row_result.get(3),
          }),
          Err(_) => None,
      }
   }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_make() {
        assert_eq!(true, true);
        let mut conn = build_database(None).unwrap();
        let node1 = Node::create(&conn, json!({"name": "test node"}));
        let node2 = Node::create(&conn, json!({"name": "test node the second"}));
        assert_eq!(node1.id, Some(1));
        assert_eq!(node2.id, Some(2));
    }
    
    #[test]
    fn triple_make() {
        let mut conn = build_database(None).unwrap();
        Node::create(&conn, json!({"name": "Boris the"}));
        Node::create(&conn, json!({"name": "Brick Top"}));
        Node::create(&conn, json!({"name": "Knows"}));
        let node1 = Node::get(&conn, 1).unwrap();
        let node2 = Node::get(&conn, 2).unwrap();
        let node3 = Node::get(&conn, 3).unwrap();
        println!("{:?}", node1);
        assert!(node1.data["name"].to_string().contains("Boris the"));     
        assert!(node2.data["name"].to_string().contains("Brick Top"));

        let triple = Triple::create(&conn, node1.id.unwrap(), node3.id.unwrap(), node2.id.unwrap());
        println!("{:?}", triple);
        let get_triple = Triple::get(&conn, 1).unwrap();
        assert_eq!(triple.id, get_triple.id);
        assert_eq!(triple.object_id, get_triple.object_id);
        assert_eq!(triple.subject_id, get_triple.subject_id);
        assert_eq!(triple.predicate_id, get_triple.predicate_id); 
    }
}

fn build_database(location: Option<&'static str>) -> Result<Connection, rusqlite::Error> {
    let mut conn = match location {
        Some(location) => Connection::open(Path::new(location)).unwrap(),
        None => Connection::open_in_memory().unwrap(),
    };
    
    match conn.execute("PRAGMA foreign_keys = 1", &[]) {
        Err(value) => println!("Not right {:?}", value),
        _ => {},
    }

   Ok(conn)
}

/*fn build_database(location: Option<&'static str>) -> Result<Connection, rusqlite::Error> {
    let mut conn = match location {
        Some(location) => Connection::open(Path::new(location)).unwrap(),
        None => Connection::open_in_memory().unwrap(),
    };
    
   let result = conn.execute("CREATE TABLE IF NOT EXISTS node (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       properties VARCHAR
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
}*/


