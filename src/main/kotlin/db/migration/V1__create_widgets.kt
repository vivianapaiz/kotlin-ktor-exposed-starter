package db.migration

import model.Widgets
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.transaction

class V1__create_widgets: BaseJavaMigration() {
    override fun migrate(context: Context?) {
        transaction {
            SchemaUtils.create(Widgets)

            Widgets.insert {
                it[name] = "widget one"
                it[quantity] = 27
                it[dateUpdated] = System.currentTimeMillis()
            }
            Widgets.insert {
                it[name] = "widget two"
                it[quantity] = 14
                it[dateUpdated] = System.currentTimeMillis()
            }
            Widgets.insert {
                it[name] = "widget tres"
                it[quantity] = 12
                it[dateUpdated] = System.currentTimeMillis()
            }
            Widgets.insert {
                it[name] = "widget cuatro"
                it[quantity] = 12
                it[dateUpdated] = System.currentTimeMillis()
            }
            Widgets.insert {
                it[name] = "widget cinco"
                it[quantity] = 12
                it[dateUpdated] = System.currentTimeMillis()
            }
            Widgets.insert {
                it[name] = "widget seis"
                it[quantity] = 12
                it[dateUpdated] = System.currentTimeMillis()
            }
            Widgets.insert {
                it[name] = "widget siete"
                it[quantity] = 12
                it[dateUpdated] = System.currentTimeMillis()
            }
            Widgets.insert {
                it[name] = "widget ocho"
                it[quantity] = 12
                it[dateUpdated] = System.currentTimeMillis()
            }
            Widgets.insert {
                it[name] = "widget nueve"
                it[quantity] = 0
                it[dateUpdated] = System.currentTimeMillis()
            }
        }
    }
}
