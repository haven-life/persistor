
var nextId = 1;
var objectTemplate;

import {PersistObjectTemplate} from './PersistObjectTemplate';
// var supertype = require('supertype');
import * as supertype from 'supertype';
export default function indexFunct(baseClassForPersist: typeof supertype.default) {
    let PersistorOT = new PersistObjectTemplate();

    require('./api.js')(PersistorOT, baseClassForPersist);
    require('./schema.js')(PersistorOT);
    require('./util.js')(PersistorOT);
    require('./mongo/query.js')(PersistorOT);
    require('./mongo/update.js')(PersistorOT);
    require('./mongo/db.js')(PersistorOT);
    require('./knex/query.js')(PersistorOT);
    require('./knex/update.js')(PersistorOT);
    require('./knex/db.js')(PersistorOT);

    return  PersistorOT; 
}

let ObjectTemplate = supertype.default;

export class Persistor { // for tests only
    static create() {
        return indexFunct(ObjectTemplate); // @TODO: add indexFunct (default persistor export)
    }
}