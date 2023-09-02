package service

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import model.ChangeType
import model.NewWidget
import model.Notification
import model.Widget
import model.WidgetNotification
import model.Widgets
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.update
import org.slf4j.LoggerFactory
import service.DatabaseFactory.dbExec

class WidgetService {

    private val listeners = mutableMapOf<Int, suspend (WidgetNotification) -> Unit>()
    private val logger = LoggerFactory.getLogger("WidgetServices")

    fun addChangeListener(id: Int, listener: suspend (WidgetNotification) -> Unit) {
        listeners[id] = listener
    }

    fun removeChangeListener(id: Int) = listeners.remove(id)

    private suspend fun onChange(type: ChangeType, id: Int, entity: Widget? = null) {
        listeners.values.forEach {
            it.invoke(Notification(type, id, entity))
        }
    }

    suspend fun getAllWidgets(): List<Widget> = dbExec {
        Widgets.selectAll().map { toWidget(it) }
    }

    suspend fun getWidget(id: Int): Widget? = dbExec {
        Widgets.select {
            (Widgets.id eq id)
        }.map { toWidget(it) }
            .singleOrNull()
    }

    suspend fun updateWidget(widget: NewWidget): Widget? {
        val id = widget.id
        return if (id == null) {
            addWidget(widget)
        } else {
            dbExec {
                Widgets.update({ Widgets.id eq id }) {
                    it[name] = widget.name
                    it[quantity] = widget.quantity
                    it[dateUpdated] = System.currentTimeMillis()
                }
            }
            getWidget(id).also {
                onChange(ChangeType.UPDATE, id, it)
            }
        }
    }

    suspend fun addWidget(widget: NewWidget): Widget {
        var key = 0
        dbExec {
            key = (
                Widgets.insert {
                    it[name] = widget.name
                    it[quantity] = widget.quantity
                    it[dateUpdated] = System.currentTimeMillis()
                } get Widgets.id
                )
        }
        return getWidget(key)!!.also {
            onChange(ChangeType.CREATE, key, it)
        }
    }

    suspend fun deleteWidget(id: Int): Boolean {
        return dbExec {
            Widgets.deleteWhere { Widgets.id eq id } > 0
        }.also {
            if (it) onChange(ChangeType.DELETE, id)
        }
    }

    private fun toWidget(row: ResultRow): Widget =
        Widget(
            id = row[Widgets.id],
            name = row[Widgets.name],
            quantity = row[Widgets.quantity],
            dateUpdated = row[Widgets.dateUpdated],
        )

    suspend fun getFastAllWidgets(ids: List<Int>): List<Widget> = withContext(Dispatchers.IO) {
        val widgets = mutableListOf<Widget>()
        ids.forEach { id ->
            launch {
                getWidget(id)?.let(widgets::add)
            }
        }
        widgets
    }

    suspend fun getNonFastAllWidgets(ids: List<Int>): List<Widget> {
        val widgets = mutableListOf<Widget>()
        ids.forEach { id ->
            getWidget(id)?.let(widgets::add)
            logger.info("Finish to get widget id $id")
        }
        return widgets
    }
}
