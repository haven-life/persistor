
var nextId = 1;
var objectTemplate;

import {PersistObjectTemplate} from './PersistObjectTemplate';
import { ObjectTemplate } from 'supertype'

export default function start(baseClassForPersist: typeof ObjectTemplate) {
    let PersistorOT = PersistObjectTemplate.initialize(baseClassForPersist);

    // require('./api.js')(PersistorOT, baseClassForPersist); - done
    // require('./schema.js')(PersistorOT);
    require('./util.js')(PersistorOT);
    require('./mongo/query.js')(PersistorOT);
    require('./mongo/update.js')(PersistorOT);
    require('./mongo/db.js')(PersistorOT);
    require('./knex/query.js')(PersistorOT);
    require('./knex/update.js')(PersistorOT);
    require('./knex/db.js')(PersistorOT);

    return  PersistorOT; 
}

export class Persistor { // for tests only
    static create() {
        return start(ObjectTemplate); // @TODO: add indexFunct (default persistor export)
    }
}