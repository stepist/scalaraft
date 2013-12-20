package jkm.cineclub.raft.Simulation

/**
 * Created with IntelliJ IDEA.
 * User: cineclub
 * Date: 12/20/13
 * Time: 1:15 AM
 * To change this template use File | Settings | File Templates.
 */

import scala.swing._
import scala.swing.BorderPanel.Position._
import event._
import java.awt.{Color, Graphics2D}
import scala.util.Random
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem

class Simulator {

}
object Simulator extends SimpleSwingApplication {

  val conf = ConfigFactory.load("applicationSim.conf")
  val system=ActorSystem.create("Sim",conf)

  import akka.actor.Actor
  import akka.actor.Props
  import akka.event.Logging

  class MyActor extends Actor {
    val log = Logging(context.system, this)
    def receive = {
      case "test" ⇒ {
        val x = Random.nextInt(100)
        val y = Random.nextInt(100)
        val c = new Color(Random.nextInt(Int.MaxValue))
        canvas.throwDart(new Dart(x, y, c))
      }
      case _      ⇒ log.info("received unknown message")
    }
  }
  val myActor = system.actorOf(Props[MyActor], "SimActor")

  val label = new Label {
    text = "I'm a big label!."
    font = new Font("Ariel", java.awt.Font.ITALIC, 24)
  }
  val button = new Button {
    text = "Throw!"
    foreground = Color.blue
    background = Color.red
    borderPainted = true
    enabled = true
    tooltip = "Click to throw a dart"
  }
  val toggle = new ToggleButton { text = "Toggle" }
  val checkBox = new CheckBox { text = "Check me" }
  val textField = new TextField {
    columns = 10
    text = "Click on the target!"
  }
  val textArea = new TextArea {
    text = "initial text\nline two"
    background = Color.green
  }
  val canvas = new Canvas {
    preferredSize = new Dimension(100, 100)
  }
  val gridPanel = new GridPanel(1, 2) {
    contents += checkBox
    contents += label
    contents += textArea
  }

  def top = new MainFrame { // top is a required method
    title = "A Sample Scala Swing GUI"
     /*
    // declare Components here
    val label = new Label {
      text = "I'm a big label!."
      font = new Font("Ariel", java.awt.Font.ITALIC, 24)
    }
    val button = new Button {
      text = "Throw!"
      foreground = Color.blue
      background = Color.red
      borderPainted = true
      enabled = true
      tooltip = "Click to throw a dart"
    }
    val toggle = new ToggleButton { text = "Toggle" }
    val checkBox = new CheckBox { text = "Check me" }
    val textField = new TextField {
      columns = 10
      text = "Click on the target!"
    }
    val textArea = new TextArea {
      text = "initial text\nline two"
      background = Color.green
    }
    val canvas = new Canvas {
      preferredSize = new Dimension(100, 100)
    }
    val gridPanel = new GridPanel(1, 2) {
      contents += checkBox
      contents += label
      contents += textArea
    } */

    // choose a top-level Panel and put components in it
    // Components may include other Panels
    contents = new BorderPanel {
      layout(gridPanel) = North
      layout(button) = West
      layout(canvas) = Center
      layout(toggle) = East
      layout(textField) = South
    }
    size = new Dimension(300, 200)
    menuBar = new MenuBar {
      contents += new Menu("File") {
        contents += new MenuItem(Action("Exit") {
          sys.exit(0)
        })
      }
    }

    // specify which Components produce events of interest
    listenTo(button)
    listenTo(toggle)
    listenTo(canvas.mouse.clicks)

    // react to events
    reactions += {
      case ButtonClicked(component) if component == button =>
        val x = Random.nextInt(100)
        val y = Random.nextInt(100)
        val c = new Color(Random.nextInt(Int.MaxValue))
        canvas.throwDart(new Dart(x, y, c))
        textField.text = s"Dart thrown at $x, $y"
      case ButtonClicked(component) if component == toggle =>
        toggle.text = if (toggle.selected) "On" else "Off"
      case MouseClicked(_, point, _, _, _) =>
        canvas.throwDart(new Dart(point.x, point.y, Color.black))
        textField.text = (s"You clicked in the Canvas at x=${point.x}, y=${point.y}.")
    }
  }
}


case class Dart(val x: Int, val y: Int, val color: java.awt.Color)

import scala.swing.Panel
import java.awt.{ Graphics2D, Color }

class Canvas extends Panel {
  var centerColor = Color.yellow

  var darts = List[Dart]()

  override def paintComponent(g: Graphics2D) {

    // Start by erasing this Canvas
    g.clearRect(0, 0, size.width, size.height)

    // Draw background here
    g.setColor(Color.blue)
    g.fillOval(0, 0, 100, 100)
    g.setColor(Color.red)
    g.fillOval(20, 20, 60, 60)
    g.setColor(centerColor)
    g.fillOval(40, 40, 20, 20)

    // Draw things that change on top of background
    for (dart <- darts) {
      g.setColor(dart.color)
      g.fillOval(dart.x, dart.y, 10, 10)
    }
  }

  /** Add a "dart" to list of things to display */
  def throwDart(dart: Dart) {
    darts = darts :+ dart
    // Tell Scala that the display should be repainted
    repaint()
  }
}

object SimTest extends App {
  val system=ActorSystem.create("test")
  val selection =
    system.actorSelection("akka.tcp://Sim@127.0.0.1:3010/user/SimActor")

  for(i <- 1 to 20 ) {
    Thread.sleep(2000)
    selection ! "test"
  }
  Thread.sleep(10000)


  system.shutdown()

}