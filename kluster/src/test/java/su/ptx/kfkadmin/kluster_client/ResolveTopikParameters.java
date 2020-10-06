package su.ptx.kfkadmin.kluster_client;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;
import su.ptx.kfkadmin.kluster.Kluster;

import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collection;

final class ResolveTopikParameters extends TypeBasedParameterResolver<Kluster.Topik> implements AfterTestExecutionCallback {
  private Kluster kluster;

  void setUp(Kluster kluster) {
    this.kluster = kluster;
  }

  void tearDown() {
    kluster = null;
  }

  @Override
  public Kluster.Topik resolveParameter(ParameterContext pc, ExtensionContext ec) throws ParameterResolutionException {
    Parameter p = pc.getParameter();
    if (!p.isNamePresent()) {
      throw new ParameterResolutionException(p + ": no name present");
    }
    String name = p.getName();
    return CreatedTopiks.get(ec).created(kluster.createTopik(name));
  }

  @Override
  public void afterTestExecution(ExtensionContext ec) {
    CreatedTopiks.get(ec).deleteAll();
  }

  private static class CreatedTopiks {
    private final Collection<Kluster.Topik> topiks = new ArrayList<>();

    private Kluster.Topik created(Kluster.Topik topik) {
      topiks.add(topik);
      return topik;
    }

    private void deleteAll() {
      topiks.forEach(Kluster.Topik::delete);
      topiks.clear();
    }

    private static CreatedTopiks get(ExtensionContext ec) {
      return ec.getStore(ExtensionContext.Namespace.GLOBAL).getOrComputeIfAbsent(CreatedTopiks.class);
    }
  }
}
