
var nextId = 1;
var objectTemplate;

export let db: knex | MongoClient;

import {PersistObjectTemplate} from './PersistObjectTemplate';
import { ObjectTemplate } from 'supertype'
import { MongoClientOptions } from 'mongodb';
import * as MongoClient from 'mongodb-bluebird';
import * as knex from 'knex';

enum DriverType {
    KNEX = 'knex',
    MONGO = 'mongo'
}

export default function start(baseClassForPersist: typeof ObjectTemplate) {
    let PersistorOT = PersistObjectTemplate.initialize(baseClassForPersist);

    // require('./api.js')(PersistorOT, baseClassForPersist); - done
    // require('./schema.js')(PersistorOT);
    // require('./util.js')(PersistorOT);
    // require('./mongo/query.js')(PersistorOT);
    // require('./mongo/update.js')(PersistorOT);
    // require('./mongo/db.js')(PersistorOT);
    // require('./knex/query.js')(PersistorOT);
    // require('./knex/update.js')(PersistorOT);
    require('./knex/db.js')(PersistorOT);

    return  PersistorOT; 
}

export class Persistor { // for tests only
    static create() {
        return start(ObjectTemplate); // @TODO: add indexFunct (default persistor export)
    }
}

export class Setup {
    async connect(url: string, options?: MongoClientOptions): Promise<MongoClient>;
    
    async connect(config: knex.Config): Promise<knex>;

    async connect(urlOrConfig: string | knex.Config, options?: MongoClientOptions) {
        if (typeof urlOrConfig === 'string') {
            db = await MongoClient.connect(urlOrConfig, options);
        }
        else {
            db = await knex(urlOrConfig);
        }
        return db;
    }
}