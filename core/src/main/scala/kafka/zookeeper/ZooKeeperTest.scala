package kafka.zookeeper

import java.util.Locale
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.zookeeper.{WatchedEvent, Watcher}
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}

import scala.collection.mutable.Set

class ZooKeeperTest(connectString: String,
                    sessionTimeoutMs: Int,
                    connectionTimeoutMs: Int) extends Logging with KafkaMetricsGroup {

  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]().asScala
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]().asScala
  private val stateChangeHandlers = new ConcurrentHashMap[String, StateChangeHandler]().asScala

  private val metricNames = Set[String]()

  // The state map has to be created before creating ZooKeeper since it's needed in the ZooKeeper callback.
  private val stateToMeterMap = {
    import KeeperState._
    val stateToEventTypeMap = Map(
      Disconnected -> "Disconnects",
      SyncConnected -> "SyncConnects",
      AuthFailed -> "AuthFailures",
      ConnectedReadOnly -> "ReadOnlyConnects",
      SaslAuthenticated -> "SaslAuthentications",
      Expired -> "Expires"
    )
    stateToEventTypeMap.map { case (state, eventType) =>
      val name = s"ZooKeeper${eventType}PerSec"
      metricNames += name
      state -> newMeter(name, eventType.toLowerCase(Locale.ROOT), TimeUnit.SECONDS)
    }
  }


  // package level visibility for testing only
  private[zookeeper] object ZooKeeperClientWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      debug(s"Received event: $event")
      Option(event.getPath) match {
        case None =>
          val state = event.getState
          stateToMeterMap.get(state).foreach(_.mark())
          inLock(isConnectedOrExpiredLock) {
            isConnectedOrExpiredCondition.signalAll()
          }
          if (state == KeeperState.AuthFailed) {
            error("Auth failed.")
            stateChangeHandlers.values.foreach(_.onAuthFailure())
          } else if (state == KeeperState.Expired) {
            scheduleSessionExpiryHandler()
          }
        case Some(path) =>
          (event.getType: @unchecked) match {
            case EventType.NodeChildrenChanged => zNodeChildChangeHandlers.get(path).foreach(_.handleChildChange())
            case EventType.NodeCreated => zNodeChangeHandlers.get(path).foreach(_.handleCreation())
            case EventType.NodeDeleted => zNodeChangeHandlers.get(path).foreach(_.handleDeletion())
            case EventType.NodeDataChanged => zNodeChangeHandlers.get(path).foreach(_.handleDataChange())
          }
      }
    }
  }

}

trait StateChangeHandler {
  val name: String
  def beforeInitializingSession(): Unit = {}
  def afterInitializingSession(): Unit = {}
  def onAuthFailure(): Unit = {}
}

trait ZNodeChangeHandler {
  val path: String
  def handleCreation(): Unit = {}
  def handleDeletion(): Unit = {}
  def handleDataChange(): Unit = {}
}

trait ZNodeChildChangeHandler {
  val path: String
  def handleChildChange(): Unit = {}
}
