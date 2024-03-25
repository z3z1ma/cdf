import typing as t

from dlt.common.configuration.providers import ConfigProvider, get_key_name

if t.TYPE_CHECKING:
    import dynaconf


class CDFConfigProvider(ConfigProvider):
    def __init__(self, settings: "dynaconf.LazySettings") -> None:
        self._settings = settings
        super().__init__()

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        return get_key_name(key, ".", *sections)

    def get_value(
        self, key: str, hint: t.Type[t.Any], pipeline_name: str, *sections: str
    ) -> t.Tuple[t.Optional[t.Any], str]:
        fqn = self.get_key_name(key, pipeline_name, *sections)
        try:
            return self._settings[fqn], fqn
        except KeyError:
            return None, fqn


class ConfigProvider(abc.ABC):
    @abc.abstractmethod
    def get_value(
        self, key: str, hint: Type[Any], pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        pass

    def set_value(
        self, key: str, value: Any, pipeline_name: str, *sections: str
    ) -> None:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def supports_secrets(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def supports_sections(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @property
    def is_empty(self) -> bool:
        return False

    @property
    def is_writable(self) -> bool:
        return False


class BaseTomlProvider(ConfigProvider):
    def __init__(self, toml_document: TOMLContainer) -> None:
        self._toml = toml_document

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        return get_key_name(key, ".", *sections)

    def get_value(
        self, key: str, hint: Type[Any], pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        full_path = sections + (key,)
        if pipeline_name:
            full_path = (pipeline_name,) + full_path
        full_key = self.get_key_name(key, pipeline_name, *sections)
        node: Union[TOMLContainer, TOMLItem] = self._toml
        try:
            for k in full_path:
                if not isinstance(node, dict):
                    raise KeyError(k)
                node = node[k]
            rv = node.unwrap() if isinstance(node, (TOMLContainer, TOMLItem)) else node
            return rv, full_key
        except KeyError:
            return None, full_key

    def set_value(
        self, key: str, value: Any, pipeline_name: str, *sections: str
    ) -> None:
        if pipeline_name:
            sections = (pipeline_name,) + sections

        if isinstance(value, TOMLContainer):
            if key is None:
                self._toml = value
            else:
                # always update the top document
                # TODO: verify that value contains only the elements under key
                update_dict_nested(self._toml, value)
        else:
            if key is None:
                raise ValueError("dlt_secrets_toml must contain toml document")

            master: TOMLContainer
            # descend from root, create tables if necessary
            master = self._toml
            for k in sections:
                if not isinstance(master, dict):
                    raise KeyError(k)
                if k not in master:
                    master[k] = tomlkit.table()
                master = master[k]  # type: ignore
            if isinstance(value, dict):
                # remove none values, TODO: we need recursive None removal
                value = {k: v for k, v in value.items() if v is not None}
                # if target is also dict then merge recursively
                if isinstance(master.get(key), dict):
                    update_dict_nested(master[key], value)  # type: ignore
                    return
            master[key] = value

    @property
    def supports_sections(self) -> bool:
        return True

    @property
    def is_empty(self) -> bool:
        return len(self._toml.body) == 0


class StringTomlProvider(BaseTomlProvider):
    def __init__(self, toml_string: str) -> None:
        super().__init__(StringTomlProvider.loads(toml_string))

    def update(self, toml_string: str) -> None:
        self._toml = self.loads(toml_string)

    def dumps(self) -> str:
        return tomlkit.dumps(self._toml)

    @staticmethod
    def loads(toml_string: str) -> tomlkit.TOMLDocument:
        return tomlkit.parse(toml_string)

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def name(self) -> str:
        return "memory"
