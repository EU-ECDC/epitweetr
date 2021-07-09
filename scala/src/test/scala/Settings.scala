package org.ecdc.epitweetr.test

import org.ecdc.epitweetr._

trait SettingsTest extends UnitTest {
  "Default splitter" should "split proper spaces" in {
    assert("hola como estas".split(Settings.defaultSplitter).toSeq.filter(_.size > 0) == Seq("hola", "como", "estas"))
  }
  it should "split CamelCase" in {
    assert("hola ComoEstas".split(Settings.defaultSplitter).toSeq.filter(_.size > 0) == Seq("hola", "Como", "Estas"))
  }

  it should "remove URLS" in {
    assert("hola como estas http://www.uc.cl http://www.saltodelpez.cl/index.php?lala=lolo".split(Settings.defaultSplitter).toSeq.filter(_.size > 0) == Seq("hola", "como", "estas"))
  }

  it should "remove twitter stop words" in {
    assert("hola como estas via RT".split(Settings.defaultSplitter).toSeq.filter(_.size > 0) == Seq("hola", "como", "estas"))
  }
  it should "ignore closest cases" in {
    assert("Hola Como httppopo viaducto RTO".split(Settings.defaultSplitter).toSeq.filter(_.size > 0) == Seq("Hola", "Como", "httppopo", "viaducto", "RTO"))
  }
  "Settings" should "be able to find EPI_HOME" in {
     assert(!sys.env.get("EPI_HOME").isEmpty) 
  }
  it should "be able to load" in {
    assume(!sys.env.get("EPI_HOME").isEmpty)
    val conf = Settings(sys.env("EPI_HOME"))
    conf.load
  }

}
