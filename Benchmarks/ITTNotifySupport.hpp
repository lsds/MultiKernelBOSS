#ifdef WITH_ITT_NOTIFY
#include <ittnotify.h>
#include <sstream>
#endif // WITH_ITT_NOTIFY

class VTuneAPIInterface {
#ifdef WITH_ITT_NOTIFY
  ___itt_domain* domain;
#endif // WITH_ITT_NOTIFY

  friend class VTuneSubtask;

public:
  explicit VTuneAPIInterface(char const* name)
#ifdef WITH_ITT_NOTIFY
      : domain(__itt_domain_create(name))
#endif // WITH_ITT_NOTIFY
            {};
  template <typename... DescriptorTypes>
  void startSampling(DescriptorTypes... tasknameComponents) const {
#ifdef WITH_ITT_NOTIFY
    std::stringstream taskname;
    (taskname << ... << tasknameComponents);
    auto task = __itt_string_handle_create(taskname.str().c_str());
    __itt_resume();
    __itt_task_begin(domain, __itt_null, __itt_null, task);
#endif // WITH_ITT_NOTIFY
  }
  void stopSampling() const {
#ifdef WITH_ITT_NOTIFY
    __itt_task_end(domain);
    __itt_pause();
#endif // WITH_ITT_NOTIFY
  }

protected:
#ifdef WITH_ITT_NOTIFY
  ___itt_domain* getDomain() const { return domain; }
#endif // WITH_ITT_NOTIFY
};

class VTuneSubtask {
#ifdef WITH_ITT_NOTIFY
  ___itt_domain* domain;
  __itt_string_handle* task;
#endif // WITH_ITT_NOTIFY

public:
  explicit VTuneSubtask(VTuneAPIInterface const& vtuneInterface, char const* name)
#ifdef WITH_ITT_NOTIFY
      : domain(vtuneInterface.getDomain()), task(__itt_string_handle_create(name))
#endif // WITH_ITT_NOTIFY
                                                {};
  void begin() const {
#ifdef WITH_ITT_NOTIFY
    __itt_task_begin(domain, __itt_null, __itt_null, task);
#endif // WITH_ITT_NOTIFY
  }
  void end() const {
#ifdef WITH_ITT_NOTIFY
    __itt_task_end(domain);
#endif // WITH_ITT_NOTIFY
  }
};

class ITTSection {
private:
  VTuneSubtask const& subtask;

public:
  explicit ITTSection(VTuneSubtask const& subtask) : subtask(subtask) { subtask.begin(); }
  ~ITTSection() { subtask.end(); }
  void pause() { subtask.end(); }
  void resume() { subtask.begin(); }
};
