#include "Optimizer.hpp"

std::unordered_map<std::string, void*> Optimizer::g_loadedLibraries;
std::unordered_map<std::string, std::string> Optimizer::g_loadedEngines;
std::vector<std::function<Expression(size_t)>> Optimizer::tasks;
std::vector<void*> Optimizer::taskArgs;
std::vector<Expression> Optimizer::resultExprs;


std::vector<Expression> Optimizer::ExecuteTasks(std::string mdFile) {
  // std::vector<Expression> resultExprs;
  // MainArgs mainArgs(resultExprs, mp);
  const CHAR *mdFileChar = mdFile.c_str();
  InpMainTask inpMainTask(mdFileChar, mp);


  struct gpos_init_params params = {NULL};
  gpos_exec_params exec_params;
  exec_params.func = MainTask;
  exec_params.arg = &inpMainTask;
  exec_params.stack_start = &params;
  exec_params.error_buffer = NULL;
  exec_params.error_buffer_size = -1;
  exec_params.abort_requested = NULL;

  int result = 0;
  if (gpos_exec(&exec_params)) {
    result = 1;
  }


  std::vector<Expression> returnResultExprs = std::move(resultExprs);
  tasks.clear();
  taskArgs.clear();
  resultExprs.clear();

  return returnResultExprs;
}

void *Optimizer::MainTask(void *arg) {
  InpMainTask *inpMainTask = static_cast<InpMainTask *>(arg);
  CMemoryPool *mp = inpMainTask->mp;
  const CHAR* mdFile = inpMainTask->mdFile;

  ConfigureEnvironment(mdFile);
  // the default MD file.
  CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
  pmdp->AddRef();
  CMDAccessor mda(mp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

  orcaextender::DynamicRegistry *dynamicRegistry =
      orcaextender::DynamicRegistry::GetInstance();
  orcaextender::BOSSCostModel *costModel = dynamicRegistry->GetCostModel();

  CAutoOptCtxt aoc(mp, &mda, NULL, /* pceeval */
                  costModel);

  for (size_t i = 0; i < tasks.size(); ++i) {
    resultExprs.push_back(tasks[i](i));
  }


  FreeAllEngines();
  GPOS_DELETE(orcaextender::DynamicRegistry::GetInstance());
  CXformFactory::Pxff()->Shutdown();
  return NULL;
}

Expression Optimizer::GetOperators(size_t idx) {
  EmptyArgs* args = static_cast<EmptyArgs *>(taskArgs[idx]);
  delete args;
  return std::move(getOperatorList());
}


Expression Optimizer::LoadLib(size_t idx) {
  using CreateEngineFn =
      orcaextender::Engine *(*)();


  LibArgs *libArgs = static_cast<LibArgs *>(taskArgs[idx]);
  std::string libPath = libArgs->libPath;

  // Check if already loaded
  if (g_loadedEngines.find(libPath) != g_loadedEngines.end()) {
    delete libArgs;
    return Symbol{"success"};
  }

  // Load the dynamic library
  // dynamic loading time experiment:
  // struct timespec t0, t1;
  // clock_gettime(CLOCK_MONOTONIC, &t0);
  // void *handle1 = dlopen(libPath.c_str(), RTLD_LAZY);
  // CreateEngineFn createEngine1 = (CreateEngineFn)dlsym(handle1, "CreateEngine");
  // orcaextender::Engine* engine1 = createEngine1();
  // engine1->Register();
  // clock_gettime(CLOCK_MONOTONIC, &t1);
  // long ms = (t1.tv_sec - t0.tv_sec) * 1000
  //         + (t1.tv_nsec - t0.tv_nsec);
  // printf("registration took %ld ns\n", ms);


  void *handle = dlopen(libPath.c_str(), RTLD_LAZY);
  if (!handle) {
    std::cerr << "Error loading library: " << dlerror() << std::endl;
    delete libArgs;
    return Symbol{"failed"};
  }

  // Store the library handle
  g_loadedLibraries[libPath] = handle;

  CreateEngineFn createEngine = (CreateEngineFn)dlsym(handle, "CreateEngine");

  if (!createEngine) {
    std::cerr << "Error finding CreateEngine function: " << dlerror()
              << std::endl;
    dlclose(handle);
    g_loadedLibraries.erase(libPath);
    delete libArgs;
    return Symbol{"failed"};
  }

  // Create the engine instance
  orcaextender::Engine* engine = createEngine();
  if (engine) {
    g_loadedEngines[libPath] = engine->GetEngineName();
    engine->Register();
    delete engine;
  } else {
    std::cerr << "Error creating engine:" << std::endl;
    dlclose(handle);
    g_loadedLibraries.erase(libPath);
    delete libArgs;
    return Symbol{"failed"};
  }

  delete libArgs;
  return Symbol{"success"};
}


Expression Optimizer::UnloadLib(size_t idx) {
  LibArgs *libArgs = static_cast<LibArgs *>(taskArgs[idx]);
  std::string libPath = libArgs->libPath;

  if (g_loadedEngines.find(libPath) == g_loadedEngines.end()) {
    return Symbol{"failed"};
  }

  std::string engineName = g_loadedEngines[libPath];
  orcaextender::DynamicRegistry *dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  dynamicRegistry->RemoveEngine(engineName);
  g_loadedEngines.erase(libPath);
  g_loadedLibraries.erase(libPath);

  delete libArgs;
  return Symbol{"success"};
}



void Optimizer::FreeAllEngines() {
  g_loadedEngines.clear();
  g_loadedLibraries.clear();
}


ComplexExpression Optimizer::getColumnsAsList(std::vector<std::string> const &columnNames) {
    ExpressionArguments args;
  std::transform(columnNames.begin(), columnNames.end(),
                 std::back_inserter(args),
                 [](auto &col) { return Symbol{col}; });
  return {"List"_, {}, std::move(args), {}};
}


boss::Expression Optimizer::getOperatorList() {
    // Get the registry
  orcaextender::DynamicRegistry *registry =
      orcaextender::DynamicRegistry::GetInstance();
  auto allOperatorsMap = registry->GetAllOperators();

  // Create arguments for the result expression
  ExpressionArguments engineExpressions;

  // Iterate over the engines and their operators
  for (const auto &[engineName, operatorNames] : allOperatorsMap) {
    // Create arguments for this engine's operators
    ExpressionArguments operatorExpressions;

    // Add all operators to the engine's argument list
    for (const auto &opName : operatorNames) {
      operatorExpressions.push_back(Symbol{opName});
    }

    // Create engine expression with its operators and add to the result's
    // arguments
    engineExpressions.push_back(ComplexExpression{
        Symbol{engineName}, {}, std::move(operatorExpressions), {}});
  }

  // Create the final result expression
  return ComplexExpression{
      Symbol{"OperatorList"}, {}, std::move(engineExpressions), {}};
}


void Optimizer::Init() {
  struct gpos_init_params params = {NULL};
  gpos_init(&params);
  gpdxl_init();
  gpopt_init();

  CAutoMemoryPool amp;
  mp = amp.Detach();

  orcaextender::BOSSCostModel *costModel = GPOS_NEW(mp) orcaextender::BOSSCostModel(mp, 1);
  orcaextender::DynamicRegistry::Init(mp, costModel);
}


void Optimizer::ConfigureEnvironment(const CHAR *mdFile) {
    // initialize DXL support
  InitDXL();
  if (CMDCache::Pcache() == NULL) {
    CMDCache::Init();
  }

  // load metadata objects into provider file. NOTE this is a default MD file. Empty it before if not wanted.
  {
    CAutoMemoryPool amp;
    CMemoryPool *mp = amp.Pmp();
    if (CTestUtils::m_pmdpf == NULL) {
      CTestUtils::InitProviderFile(mp, mdFile);
    }
    // detach safety
    (void)amp.Detach();
  }

  // #ifdef GPOS_DEBUG
  //   // reset xforms factory to exercise xforms ctors and dtors
  //   CXformFactory::Pxff()->Shutdown();
  //   GPOS_RESULT eres = CXformFactory::Init();

  //   GPOS_ASSERT(GPOS_OK == eres);
  // #endif  // GPOS_DEBUG
}



void Optimizer::Cleanup() {
  std::cout << "Cleaning up" << std::endl;
  // gpopt_terminate();
  // gpdxl_terminate();
  // gpos_terminate();
  // CMDCache::Shutdown();
  // CTestUtils::DestroyMDProvider();
}