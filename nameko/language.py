from nameko.dependencies import InjectionProvider


LANGUAGE_CONTEXT_KEY = 'language'


class LanguageProvider(InjectionProvider):

    def acquire_injection(self, worker_ctx):
        return worker_ctx.data.get(LANGUAGE_CONTEXT_KEY)
